#!/usr/bin/perl -w
# timesheets/Update.pm

use strict;

#package timesheets::Update;

use Exporter;
use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);
$VERSION    = do { my @r = (q$Revision: 1.00 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, (@r?@r:0) }; # must be all one line, for MakeMaker
@ISA         = qw(Exporter);
@EXPORT      = qw(
   &TimeUpdate &TimeDeleteItem &TimeMessage
   );
@EXPORT_OK      = qw();
%EXPORT_TAGS   = ();

use Carp;
use Tal;
use Tal::Browser;
use Tal::Date;
use Tal::SQL;
use Beacon::Utilities;
use Beacon::Database ;

use Beacon::Log;
use Beacon::Utilities qw/:MPLock/;
use beaspoke::BeaSpoke;
use timesheets::Readtimefiles;
use timesheets::TimeDisplay;
use View::Timecard2; 
use View::TSImage2; 
use View::TSDocTypes2; 
use View::Base2;
use View::Timedata2;
use View::Orders2;
use View::Bookings2;
use Data::Dumper;

sub TimeUpdate
{
   my $Tc = shift;
   MPLock ($Tc->{lock_file});
   unless ($Tc->{timesheet_no})
   {
      $Tc->{timesheet_no} = GetNextTimesheetNo ($Tc);

      unless ($Tc->{timesheet_no})
      {
         $Tc->{Locking} = "failed";
         MPUnlock ($Tc->{lock_file});
         return 0;
      }
   }

   TimeUpdateTimecard ($Tc);
   TimeUpdateBooking  ($Tc);
   TimeUpdateOrder    ($Tc);
   TimeUpdateTimedata ($Tc);
   TimeUpdateCand     ($Tc);
   TimeUpdateOrdstats ($Tc);

	my $imgno = GetImageNo($Tc->{timesheet_no}) ;

	$Tc->{zero_imgno_error} = 0 ;
	unless ($imgno)
	{
		$Tc->{zero_imgno_error} = 1 ;
	}
   $Tc->{success} = "Timesheet no $Tc->{timesheet_no} for $Tc->{cand_name} updated with Image $imgno";

   if ($Tc->{image_process})
   {
		if ($Tc->{image_no})
		{
			$Tc->{image_update} = "T/S Added $Tc->{timesheet_no}, $Tc->{booking_no}, $Tc->{cand_name}";
		}
		else
		{
			$Tc->{image_update} = "Warning: No Image Attached - T/S Added $Tc->{timesheet_no}, $Tc->{booking_no}, $Tc->{cand_name}";
		}
   }
   MPUnlock ($Tc->{lock_file});
}

sub GetImageNo
{

   my $timesheet_no = shift;
   my $ImageNo=0;

   my $time_db = View::Timedata2->new();

	$time_db->Find($timesheet_no);

   my $rp = $time_db->{rec_hptr};

   $ImageNo = $rp->{"ti_image_no"};

	return $ImageNo;
}

sub TimeDeleteItem
{
   my $Tc = shift;
   my %record = ();
   my $Timecard     = \%record;

   # Delete the timecard record from front/back office

	my $db = View::Timecard2->new( );
	if ( my $ret = $db->Delete( $Tc->{timesheet_no}) )
	{
   	$Tc->{message} = "Error deleting timesheet: ".$db->sql_error();
	}
	else
	{
   	$Tc->{message} = "Timesheet no $Tc->{timesheet_no} DELETED";
	my $delete = 1;
   	TimeUpdateBooking  ($Tc, $delete);
   	TimeUpdateOrder    ($Tc, $delete);
		if ( TimeDeleteImages	 ($Tc) )
		{
   		$Tc->{message} .= " - ERROR deleting attached images";
		}
	}
}

sub TimeDeleteImages
{
   my $Tc = shift;

	my $tsimagedb = View::TSImage2->new();
	return $tsimagedb->delete_timesheet_images($Tc->{timesheet_no});
}
#.....................................DATABASE UPDATES
#
# Check if MYSQL system, if so use auto_increment on timecard else
# Get next timesheet number from the branch database and insert timesheet record
#
sub GetNextTimesheetNo
{
   my $Tc = shift;
	my $db = View::Timecard2->new( );
   $Tc->{branch} = $Tc->{pr_branch};
   if ($Tc->{param_ts_branch} eq "Booking")
   {
      $Tc->{branch} = $Tc->{oa_branch};
   }
   if ($Tc->{param_ts_branch} eq "Candidate")
   {
      $Tc->{branch} = $Tc->{cand_branch};
   }
	my $ret = $db->GetNextTimesheetNo ($Tc->{branch});

   return $ret; 
}

#.........................Update the timecard record
sub TimeUpdateTimecard
{
   my $Tc = shift;
   my %record = ();

	my $db  = View::Timecard2->new();

	# SPECIAL:T/S REC IS CREATED IN BACK OFFICE ONLY SO INSERT() HERE NOT NORMALLY DONE SO ADD REC TO FO
	if ( $db->find1($Tc->{timesheet_no}) )
	{
		$db->{dbh} = $db->{mydbh};
		 my ($rv, $sth1) = $db->sql_select( 
						fieldsptr		=> [ 'tc_timesheet_no' ],
						wherefieldsptr =>  [ [ 'tc_timesheet_no', '=', $Tc->{timesheet_no} ] ],
		#				dbh 				=> $db1->{mydbh}, 
						from 				=> "timecard" );
		my $rp = $sth1->fetchrow_hashref_ptr();
		unless ($rp->{tc_timesheet_no})
		{
			# DUAL INSERT
			$db->sql_insert(fieldvaluesptr => { tc_timesheet_no => $Tc->{timesheet_no}}, dbh => $db->{mydbh}, fatal => 0); 
		}
	}
	else
	{
		# DUAL INSERT TO BO/FO
		$db->insert(fieldvaluesptr => { tc_timesheet_no => $Tc->{timesheet_no}});
	}

	$db  = View::Timecard2->new(); # Use Base2 to determine MySql/BeaSpoke
	$db->find1($Tc->{timesheet_no});

   foreach ( keys %{$db->{rec_hptr}} )
   {
      $record{"tc_"."$_"} = $Tc->{"$_"} if defined $Tc->{"$_"};
   }

   # then fill in different names
   $record{tc_cust_code}    = $Tc->{cu_cust_code} || "";
   $record{tc_surname}      = $Tc->{cand_surname} || "";
   # use sprintf to avoid totals to zero appearing as 1.2345-e15
   $record{tc_hour_total}   = sprintf( '%.2f', $Tc->{true_hrs_total} );
   $record{tc_s_hours_tot}  = sprintf( '%.2f', $Tc->{hrs_total} ) || 0;
   $record{tc_emp_gr}       = $Tc->{pay_total} - $Tc->{pay_vat_amount} || 0.0;

   $record{tc_vat_amount}  = $Tc->{pay_vat_amount} || 0.0;

   $record{tc_inv_vat_amt}  = $Tc->{inv_vat_amount} || 0.0;
   $record{tc_cust_gr}      = $Tc->{inv_total} || 0.0;
   $record{tc_consultant}   = $Tc->{oa_consultant} || "";
   $record{tc_us_code1}   	 = $Tc->{oa_us_code1} || "";
   $record{tc_us_code2}   	 = $Tc->{oa_us_code2} || "";
   $record{tc_centre}       = $Tc->{centre} || $Tc->{oa_centre} || "";
   $record{tc_custref}      = $Tc->{custref} || $Tc->{oa_custref} || "";
   $record{tc_em_desc}      = $Tc->{oa_assignment} || "";
   $record{tc_he_action}    = $Tc->{pr_he_action} || "";
   $record{tc_pay_type}     = $Tc->{pr_pay_type} || "";
   $record{tc_pay_period}   = $Tc->{pr_pay_period} || "";
   $record{tc_pl_sup}       = $Tc->{pr_supp_code} || "";
   $record{tc_initials}     = $Tc->{cand_initials} || "";
   $record{tc_knownas}      = $Tc->{cand_knownas} || "";
   $record{tc_oa_branch}    = $Tc->{oa_branch} || "" ;
   $record{tc_pr_branch}    = $Tc->{pr_branch} || "";
   $record{tc_cr_branch}    = $Tc->{cand_branch} ||"";
   if ($Tc->{oa_use_sc_name} eq "No")
   {
      $record{tc_comp_name} = $Tc->{pr_ltd_name} ||"";
   }
	if ( defined $Tc->{oa_comm_consult__2} && ( $Tc->{oa_comm_consult__2} ne "") )
	{
		$record{tc_split_comm} = "Split Comm";
	}	

   $record{tc_enter_date}   = sm_Today ();
   $Tc->{date_1}            = $record{tc_enter_date};
   $Tc->{date_2}            = $record{tc_enter_date};
   $record{tc_enter_time}   = sm_CurrentTime ();
   $record{tc_entered_by}   = $main::PARAM{user_login};
   $record{tc_enter_date}   = sm_StandardDate ($record{tc_enter_date});
   $record{tc_cu_date}      = sm_StandardDate ($record{tc_cu_date});
   # Gwynne , added the following to ensure they are logged
   $Tc->{enter_date}        = $record{tc_enter_date};    # for logging purposes
   $Tc->{entered_by}        = $record{tc_entered_by};    # for logging purposes
   $Tc->{enter_time}        = $record{tc_enter_time};    # for logging purposes


	#............................................Update limit hours if rule is UA
   foreach (1..8)
   {
	   if ($Tc->{"timesheet_rule__$_"} eq "UA")
		   {
	      if ($record{"tc_rate_hours__$_"} == 0)
			   {
            $record{"tc_rate_hours__$_"} = $record{"tc_hour_total"};
	         if ($record{"tc_rate_hours__$_"} > $Tc->{param_he_cap_hr})
			      {
               $record{"tc_rate_hours__$_"} = $Tc->{param_he_cap_hr};
		   		}
				}
			}
   }
   $db->Update($Tc->{timesheet_no}, fieldvaluesptr => \%record);
}

# Update the booking record
sub TimeUpdateBooking
{
   my $Tc   = shift;
   my $delete = shift || 0;
   my %record = ();

   $record{oa_acc_cost}     = $Tc->{inv_total} - $Tc->{last_inv_total};
   $record{oa_acc_hrs}      = $Tc->{hrs_total} - $Tc->{last_hrs_total};
   $record{oa_paylast}      = $Tc->{oa_paylast};
   $record{oa_paylast_date} = $Tc->{oa_paylast_date};
   $record{oa_max_cost}	    = $Tc->{oa_max_cost}; # READ for param_ord_warn check but don't overwrite with zero from newdata
   $record{oa_max_hrs}	    = $Tc->{oa_max_hrs};

	# if booking ring_status FIN=Finished or P45=Finished+P45 change status to Complete
	if ( ($main::CONFIG{dbsettings}->value_noerr("sysoption_booking_finish_on_timesheet")||"No") eq "Yes" )
	{
		if ( $Tc->{oa_ring_status} eq 'FIN' || $Tc->{oa_ring_status} eq 'P45' )
		{
			if ( ($main::CONFIG{dbsettings}->value_noerr("sysoption_booking_finish_by_back_office")||"Yes") eq "No" )
			{
				$record{oa_status} = 'Complete';
			}
		}
	}

	# Read Booking

	my $bkg_db = View::Bookings2->new();
	if ( $bkg_db->Find($Tc->{booking_no}) )
	{
		UpdateBookingsHours($Tc, $bkg_db->{rec_hptr}, \%record, $delete);
	}

	# Update Booking
	$bkg_db->Update($Tc->{booking_no}, \%record);
}

sub UpdateBookingsHours
{
   my $Tc      = shift;
   my $record  = shift;
   my $newdata = shift;
   my $delete  = shift || 0; # Reduce cost/hours if timecard being deleted

   # Dont set these to zero, they are not changed here just used to CheckBookingHours
   $newdata->{oa_max_cost} = $record->{oa_max_cost};
   $newdata->{oa_max_hrs}  = $record->{oa_max_hrs}; 
   if ( $delete )
   {
   	$newdata->{oa_acc_cost} = $record->{oa_acc_cost} - $Tc->{last_inv_total};
  		$newdata->{oa_acc_hrs}  = $record->{oa_acc_hrs}  - $Tc->{last_hrs_total};
   }
   else
   {
   	$newdata->{oa_acc_cost} = $record->{oa_acc_cost} + $Tc->{inv_total} - $Tc->{last_inv_total};
  		$newdata->{oa_acc_hrs}  = $record->{oa_acc_hrs}  + $Tc->{hrs_total} - $Tc->{last_hrs_total};
   }
}


sub CheckBookingsHours
{
   my $Tc      = shift;
   my $record  = shift;
   my $delete  = shift || 0; # Ignore if timecard being deleted

   unless ( $delete )
   {
		my $max_cost = $record->{oa_max_cost} || 0;
		if ( $max_cost > 0.0 )
		{
			my $wk_cost = $record->{oa_max_cost} * $Tc->{param_ord_warn} / 100;
			if ( $record->{oa_acc_cost} > $wk_cost )
			{
				if ( $Tc->{image_process} )
				{
					my $alertmsg = "WARNING: Actual Cost on booking exceeds ".$Tc->{param_ord_warn}."% of Max Cost";
					sm_Html("<script language=javascript>alert(\"$alertmsg\");</script>");
				}
				else
				{
					TimeMessage($Tc, "WARNING: Actual Cost on booking exceeds ".$Tc->{param_ord_warn}."% of Max Cost");
				}
				return;
			}
		}	
		my $max_hrs = $record->{oa_max_hrs} || 0;
		if ( $max_hrs > 0 )
		{
			my $wk_hrs  = $record->{oa_max_hrs} * $Tc->{param_ord_warn} / 100;
			if ( $record->{oa_acc_hrs} > $wk_hrs )
			{	
				if ( $Tc->{image_process} )
				{
					my $alertmsg = "WARNING: Actual Hours on booking exceeds ".$Tc->{param_ord_warn}."% of Max Hours";
					sm_Html("<script language=javascript>alert(\"$alertmsg\");</script>");
				}
				else
				{
					TimeMessage($Tc, "WARNING: Actual Hours on booking exceeds ".$Tc->{param_ord_warn}."% of Max Hours");
				}
			}
			return;
		}
   }
}

sub TimeUpdateOrder
{
   my $Tc   = shift;
   my $delete = shift || 0;
   my %updaterec = ();

 	my $orderobj = View::Orders2->new ();
  
	$updaterec{or_acc_cost}    = $Tc->{inv_total} - $Tc->{last_inv_total};
   $updaterec{or_acc_hrs}     = $Tc->{hrs_total} - $Tc->{last_hrs_total};
   $updaterec{or_week_no_pay} = $Tc->{or_week_no_pay};
   $updaterec{or_max_cost}	 	= $Tc->{or_max_cost};   
	$updaterec{or_max_hrs}	   = $Tc->{or_max_hrs};
	

	my %record = ();
	%record = $orderobj->Find ( $Tc->{order_no} );
	unless(%record){ # create new record
		$orderobj->insert( fieldvaluesptr =>{'or_order_no' => $Tc->{order_no} });
	}
	
	UpdateOrderHours( $Tc, \%record, \%updaterec, $delete);
 	$orderobj->Update(  $Tc->{order_no}, \%updaterec);

}


sub UpdateOrderHours
{
   my $Tc      = shift;
   my $record  = shift;
   my $newdata = shift;
   my $delete  = shift || 0; # Reduce cost/hours if timecard being deleted
   my $weekplus = $Tc->{workweek} + 10;
   my $weekless = $Tc->{workweek} - 10;
   my $weekord  = $record->{or_week_no_pay};
   my $newweek  = $weekord;
   if (($Tc->{workweek} > $weekord) && ($weekless <= $weekord))
   {
      $newweek = $Tc->{workweek};
   }
   if ($weekplus < $weekord)
   {
      $newweek = $Tc->{workweek};
   }
   if ($weekord == 0)
   {
      $newweek = $Tc->{workweek};
   }
   $newdata->{or_week_no_pay} = $newweek;

   # Dont set these to zero, they are not changed here just used to CheckOrderHours
   $newdata->{or_max_cost} = $record->{or_max_cost};
   $newdata->{or_max_hrs}  = $record->{or_max_hrs}; 

   if ( $delete )
   {
   	$newdata->{or_acc_cost} = $record->{or_acc_cost} - $Tc->{last_inv_total};
  		$newdata->{or_acc_hrs}  = $record->{or_acc_hrs}  - $Tc->{last_hrs_total};
   }
   else
   {
   	$newdata->{or_acc_cost} = $record->{or_acc_cost} + $Tc->{inv_total} - $Tc->{last_inv_total};
  		$newdata->{or_acc_hrs}  = $record->{or_acc_hrs}  + $Tc->{hrs_total} - $Tc->{last_hrs_total};
   }
}

sub CheckOrderHours
{
   my $Tc      = shift;
   my $record  = shift;
   my $delete  = shift || 0; # Ignore if timecard being deleted

   unless ( $delete )
   {
		my $max_cost = $record->{or_max_cost} || 0;
		if ( $max_cost > 0.0 )
		{
			my $wk_cost = $record->{or_max_cost} * $Tc->{param_ord_warn} / 100;
			if ( $record->{or_acc_cost} > $wk_cost )
			{			
				if ( $Tc->{image_process} )
				{
					my $alertmsg = "WARNING: Actual Cost on Order exceeds ".$Tc->{param_ord_warn}."% of Max Cost";
					sm_Html("<script language=javascript>alert(\"$alertmsg\");</script>");
				}
				else
				{
					TimeMessage($Tc, "WARNING: Actual Cost on Order exceeds ".$Tc->{param_ord_warn}."% of Max Cost");
				}
				return;
			}
		}
		my $max_hrs = $record->{or_max_hrs} || 0;
		if ( $max_hrs > 0 )
		{
			my $wk_hrs  = $record->{or_max_hrs} * $Tc->{param_ord_warn} / 100;
			if ( $record->{or_acc_hrs} > $wk_hrs )
			{	
				if ( $Tc->{image_process} )
				{
					my $alertmsg = "WARNING: Actual Hours on Order exceeds ".$Tc->{param_ord_warn}."% of Max Hours";
					sm_Html("<script language=javascript>alert(\"$alertmsg\");</script>");
				}
				else
				{
					TimeMessage($Tc, "WARNING: Actual Hours on Order exceeds ".$Tc->{param_ord_warn}."% of Max Hours");
				}
			}
			return;
		}
   }
}

#
#   Attach image to timesheet by writing a timedata record
#
sub TimeUpdateTimedata
{
   my $Tc     = shift;
   my %updaterec = ();

 	my $time_db = View::Timedata2->new();

   $updaterec{ti_timesheet_no} = $Tc->{timesheet_no};
   $updaterec{ti_payroll_no}   = $Tc->{payroll_no};
   $updaterec{ti_booking_no}   = $Tc->{booking_no};
   $updaterec{ti_image_no}     = $Tc->{image_no};
   $updaterec{ti_serial_code}  = $Tc->{serial_code};
   $updaterec{ti_override_why} = $Tc->{override_why};

   $time_db->Update( $Tc->{timesheet_no}, \%updaterec);

	if ( $Tc->{image_no} )
	{
		# Add Image/Document to New Timesheet Document/Image Table (tsimages)
		my $tsi_db = View::TSImage2->new();

		my $where = [ ['tsi_timesheet_no','=',$Tc->{timesheet_no} ], ['tsi_image_no','=',$Tc->{image_no} ] ] ;
		if ( $tsi_db->find_where( wherefieldsptr => $where ) )
		{
			return 0;
		}
		my $doctype	  	  = $Tc->{'doctype__src'}  		|| "TIM";
		#my $docname	  	  = $Tc->{'docname__src'}  		|| "";

		# Build comment field
		my $ddesc = br_field( field => "tsdoctype", value => $doctype ) || "";
		my $docname = "$ddesc for Cand $Tc->{cand_no}, $Tc->{cand_name}, on T/S $Tc->{timesheet_no}";

		my $docdb = View::TSDocTypes2->new();
		my $invoice = $docdb->include_with_invoice($doctype) ? "Yes" : "No";

		$tsi_db->{rec_hptr}->{tsi_timesheet_no} = $Tc->{timesheet_no};
		$tsi_db->{rec_hptr}->{tsi_image_no} = $Tc->{image_no};
		$tsi_db->{rec_hptr}->{tsi_doc_type} = $doctype;
		$tsi_db->{rec_hptr}->{tsi_doc_name} = $docname;
		$tsi_db->{rec_hptr}->{tsi_doc_invoice} = $invoice; 

	
  	 	unless ( $tsi_db->insert( fieldvaluesptr => $tsi_db->{rec_hptr} ) )
		{
			#$tc{message} = "Error adding image to tsimage table: ".$tsi_di->sql_error;
			return 0;
		}
	}
	return 1;
}

#.........................Update the candidate record
sub TimeUpdateCand
{
   my $Tc   = shift;
   my $Cand = shift;

   if ($Tc->{bonus_hours})
   {
      my %record = ();
      $record{"cand_bon_hrs"} = $Tc->{bonus_hours};
      UpdateRecord (
         tc       => $Tc,
         table    => "cands",
         newdata  => \%record,
         where    => "cand_cand_no = $Tc->{cand_no}");
   }
}

#.........................Update the order history record
sub TimeUpdateOrdstats
{
   my $Tc    = shift;
   my %record = ();
   $record{os_cand_no}      = $Tc->{cand_no};
   $record{os_custno}       = $Tc->{cust_code};
   $record{os_cand_seq}     = $Tc->{cand_no};
   $record{os_rate_code}    = $Tc->{rate_desc__1};
   $record{os_rate_hours}   = $Tc->{rate_hours__1};
   $record{os_rate_pay}     = $Tc->{rate_pay__1};
   $record{os_rate_invoice} = $Tc->{rate_inv__1};
   $record{os_no_hours}     = $Tc->{hrs_total} - $Tc->{last_hrs_total}; # but may add
   UpdateRecord (
      tc       => $Tc,
      table    => "ordstats",
      newdata  => \%record,
      function => \&UpdateOrdstatsHours,
      where    => "os_cand_no = $Tc->{cand_no} AND os_custno = \"$Tc->{cust_code}\"");
}

sub UpdateOrdstatsHours
{
   my $Tc     = shift;
   my $record = shift;
   my $newdata = shift;

   $newdata->{os_no_hours} = $record->{os_no_hours} + $Tc->{hrs_total} - $Tc->{last_hrs_total};
}

#
# general purpose add or update routine
#
sub UpdateRecord
{
   my %param = @_;
   my %record   = ();
   my $table    = $param{table};
   my $where    = "WHERE $param{where}";
   my $type     = $param{type};
   my $Tc       = $param{tc};
   my $function = $param{function};
   my $warn_function = $param{warn_function};
   my $delete   = $param{delete_rec};
   my $newdata  = $param{newdata};
   my $dbh      = $main::CONFIG{MODULE_MYSQL}? $main::dbh_mytal : $param{dbh} || $main::CONFIG{dbh_tal};
   my @fields = ();
   while (my ($key, $value) = each (%$newdata))
   {
      push @fields, $key;
   }
   my @sortedfields = sort @fields;
   #.............................Prepare new record if not there
   %record = $dbh->ReadSingleRow (
      fields  => \@sortedfields,
      table   => $table,
      sqlrest => $where);

   if (%record) # record already exists
   {
      if ($function)   # need to read old data to determine new data
      {
         &$function ($Tc, \%record, $newdata, $delete);
      }
      while (my ($key, $value) = each (%$newdata))
      {
         # $value ||= "";
         $value = Tal::SQL::ValidSqlChar ($value);
         $record{$key} = $value ;
      }
      my $oa_paylast1      = $record{oa_paylast}         || 0;
      my $oa_paylast_date1 = $record{oa_paylast_date}    || '';
      my $oa_paylast2      = $newdata->{oa_paylast}      || 0;
      my $oa_paylast_date2 = $newdata->{oa_paylast_date} || '';
      my $txt = $delete ? " Delete " : " Update ";
      Beacon::Log::LogIt (1, "Timesheet$txt $oa_paylast1, $oa_paylast_date1, $oa_paylast2, $oa_paylast_date2.");

		# Check data and set warning if needed
      if ($warn_function)   # need to read old data to determine new data
      {
         &$warn_function ($Tc, \%record, $delete);
      }
      # Update the row
      $dbh->UpdateRow(
         values   => \%record,
         table    => $table,
         sqlrest  => $where);
   }
   else  # create a new record
   {
      %record  = $dbh->PrepareNewRow(
         table    => $table );
      while (my ($key, $value) = each (%$newdata))
      {
         $value = Tal::SQL::ValidSqlChar ($value);
         $record{$key} = $value;
      }

		# Check data and set warning if needed
      if ($warn_function)   # need to read old data to determine new data
      {
         &$warn_function ($Tc, \%record);
      }
      $dbh->InsertRow(
         table    => $table,
         values   => \%record);
   }
}

1;
