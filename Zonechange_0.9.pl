use strict;
use warnings;
use v5.10.0;
#use Cwd;
use Data::Dumper::Perltidy;
use File::Find;
use File::Basename;
use DateTime::Format::Strptime qw( );
use POSIX qw(strftime);
use Regexp::Assemble;

our ( $ra, $match, @hash_match, %regex, %hoh );                                                                        # Global var
our $epoc = time();
$epoc = $epoc - 24 * 60 * 60;                                                                                          # One day before of current date.
our $datestring = strftime "%Y-%m-%d", localtime($epoc);

#our $in_files = "$datestring";
#our $in_files = qr(SYF-PLC[68]_$datestring);
our $in_files = qr(SYF-PLC[68]_$datestring);
our $cwd      = "c:/temp/xxxx";

#our $cwd = "d:/SyfPlc.Logs";
our $max_time_diff = 5;                                                                                                # Report batches with more than "max_time_diff" seconds between batches

sub build_regex {                                                                                                      # Compile all regex in %regex to one regex
  $ra = Regexp::Assemble->new;
  foreach my $key ( keys %regex ) {
    my $regex = $regex{$key};
    $ra->add($regex);
  }
}

sub findfiles {                                                                                                        # Used to iterate through each result
  my $file = $File::Find::name;                                                                                        # Complete path to the file
  return unless -f $file;                                                                                              # Process files (-f), not directories
  return
   unless $_ =~ m/$in_files/io;                                                                                        # Check if file matches input regex, io = case-insensitive, compiled
  $match++;
  open IN_F, '<', $file
   or print "\n* Couldn't open ${file}\n\n" && return;                                                                 # Open FileHandle for reading
  $file =~ s/^\Q$cwd//g;                                                                                               # Remove current working directory
  while ( my $line = <IN_F> ) {
    if ( $line =~ $ra ) { push @hash_match, $line }                                                                    # All matching lines from all regex in $regex
  }
  close IN_F;                                                                                                          # Close files and move on to the next result
}

sub write_files {                                                                                                      # Write log file for each regex in %regex.
  foreach my $event ( keys %regex ) {                                                                                  # Recursive cearch through CWD
    my $outfile = "$datestring" . "_$event" . ".log" or die "$!";
    unconditionaldelete($outfile);                                                                                     # Unconditional delete of file if exits
    my $rgx         = $regex{$event};
    my @regex_match = ();
    foreach my $line (@hash_match) {
      open OUT_F, '>>', $outfile;                                                                                      # Open FileHandle for writing
      if ( $line =~ $rgx ) {
        push @regex_match, $line;
      }
    }
    print OUT_F @regex_match;
    close OUT_F;
  }
}

sub build_hash {                                                                                                       # Find first$last BCR3 timestamp & total lifts for batch & first timestamp next batch
  my $match;
  my $string;
  my $timestamp = 0;
  my $id;
  my $batchid       = 0;
  my $batchid_first = 0;
  foreach (@hash_match) {
    if ( $_ =~ qr((^([-\d:.\s]+)(?=:)[^<]+<14[^X#]+[X#].{6}(.{5}).+15=(\d+)\)$)) ) {
      ( $string, $timestamp, $batchid_first, $id ) = ( $1, $2, $3, $4 );
      if ( $batchid !~ $3 ) {
        unless ( $3 eq "AD##," ) { say my $e = "Warning - Noread $3 $timestamp\n"; warning_error($e); $match = 1 }
        ;                                                                                                              # NoRead
        if ( $3 eq "AD##," ) { $batchid_first = $batchid; $match++; }
        unless ( $3 =~ qr([^_\W]{5}) ) { say my $e = "Error - Invalid BatchId $3 $timestamp\n"; warning_error($e) }

        $hoh{$batchid}{"FirstNextBatch"} = $timestamp;                                                                 # First Event in next batch
        $hoh{$batchid}{"ID"}             = $id;
        $batchid                         = $batchid_first;
        $hoh{$batchid}{"First Lift"}     = $timestamp;                                                                 # First Event in batch
        $hoh{$batchid}{"Last Lift"}      = $timestamp;                                                                 # Last Event in batch
        $hoh{$batchid}{"Total Lifts"}    = $match;
      }
      else {
        $match++;
        $hoh{$batchid}{"Last Lift"}   = $timestamp;                                                                    # Last Event in batch
        $hoh{$batchid}{"Total Lifts"} = $match;
      }
    }
  }
}

sub timediff {                                                                                                         # Calculate timediff between first $ last lift in batch,
  my $format = DateTime::Format::Strptime->new(
              pattern  => '%Y-%m-%d  %H:%M:%S.%3N',                                                                              # -and between last lift to first lift next batch
              on_error => 'croak', );
  foreach my $batchid ( sort keys %hoh ) {
    if ( $batchid ne "0" ) {
      my $sts1 = $hoh{$batchid}{"First Lift"};
      my $ets1 = $hoh{$batchid}{"Last Lift"};
      unless ( defined($sts1) ) { say my $e = "Error - No such sts timestamp $batchid"; warning_error($e); next }
      unless ( defined($ets1) ) { say my $e = "Error - No such ets timestamp $batchid"; warning_error($e); next }
      my $sdt1  = $format->parse_datetime($sts1);
      my $edt1  = $format->parse_datetime($ets1);
      my $diff1 = $edt1->subtract_datetime_absolute($sdt1)->in_units('nanoseconds') / 1e9;
      $hoh{$batchid}{"Diff1"} = $diff1;

      if ( $hoh{$batchid}{"FirstNextBatch"} ) {
        my $sts2  = $hoh{$batchid}{"Last Lift"};
        my $ets2  = $hoh{$batchid}{"FirstNextBatch"};
        my $sdt2  = $format->parse_datetime($sts2);
        my $edt2  = $format->parse_datetime($ets2);
        my $diff2 = $edt2->subtract_datetime_absolute($sdt2)->in_units('nanoseconds') / 1e9;
        $hoh{$batchid}{"Diff2"} = $diff2;
      }
    }
  }
}

sub find_shid {                                                                                                        # Find ShuttleId corresponding to first lift next batch
  foreach my $string (@hash_match) {
    foreach my $batchid ( keys %hoh ) {
      if ( $hoh{$batchid}{"ID"} ) {
        if ( $string =~ qr((^([-\d:.\s]+)(?=:)[^<]+<13[^\(]+\(ShuttleId=(\d+).+15=($hoh{$batchid}{"ID"})\)$)) ) {
          $hoh{$batchid}{"SHID"} = $3;
        }
      }
    }
  }
}

sub find_events {                                                                                                      # Find all events corresponding to first lift next batch
  foreach my $string (@hash_match) {
    foreach my $batchid ( keys %hoh ) {
      if ( defined( $hoh{$batchid}{"SHID"} ) ) {
        if ( $string =~ qr(<404.+$hoh{$batchid}{"SHID"}) ) {
          $hoh{$batchid}{"Event_TrolleyEnteredJunction"} = $string;
          next;
        }
        if ( $string =~ qr(<401.+$hoh{$batchid}{"SHID"}) ) {
          $hoh{$batchid}{"Event_TrolleyLeftJunction"} = $string;
          next;
        }
        if ( $string =~ qr(<8.+$hoh{$batchid}{"SHID"}) ) {
          $hoh{$batchid}{"Event_TrolleyEnteredWorkspace"} = $string;
          next;
        }
        if ( $string =~ qr(<13.+$hoh{$batchid}{"SHID"}) ) {
          $hoh{$batchid}{"Event_LiftUnloaded"} = $string;
          next;
        }
        if ( $string =~ qr(<14.+$hoh{$batchid}{"ID"}) ) {
          $hoh{$batchid}{"Event_LiftDetectedAtBcr3"} = $string;
        }
      }
    }
  }
}

sub report {                                                                                                           # Write report if timediff between last lift and
  my %hash    = %{hoh};                                                                                                # -  first lift next batch >= $max_time_diff
  my $outfile = "$datestring" . "_Report" . ".txt" or die "$!";
  unconditionaldelete($outfile);                                                                                       # Unconditional delete of file if exits
  open OUT_F, '>>', $outfile;                                                                                          # Open FileHandle for writing
  foreach my $batchid ( sort keys %hash ) {
    if ( defined( $hash{$batchid}{"Diff2"} ) ) {
      if ( $hash{$batchid}{"Diff2"} >= $max_time_diff ) {
        my $d1 = $hoh{$batchid}{"Diff1"};
        my $d2 = $hoh{$batchid}{"Diff2"};
        my $t1 = format_time( $d1, 1 );
        my $t2 = format_time( $d2, 2 );
        printf "\nBatchId%22s\n",             $batchid;
        printf "Total lifts in batch\t%-s\n", $hoh{$batchid}{"Total Lifts"};
        say $t1;
        say $t1;
        printf OUT_F "BatchId%26s\n",                 $batchid;
        printf OUT_F "Total lifts in batch\t\t%-s\n", $hoh{$batchid}{"Total Lifts"};
        printf OUT_F "$t1\n";
        printf OUT_F "$t2\n";
      }
    }
  }
  close OUT_F;
}

sub format_time {                                                                                                      #format time value depending on duration
  my $s  = shift;
  my $id = shift;
  if ( $id == 1 ) {
    return sprintf "Time for the Zone\t%.3f\t\tss", $s if $s < 60;
    my $m = $s / 60;
    $s = $s % 60;
    return sprintf "Time for the Zone\t%02d:%02d\t\tmm:ss", $m, $s if $m < 60;
    my $h = $m / 60;
    $m %= 60;
    return sprintf "Time for the Zone\t%02d:%02d:%02d\thh:mm:ss", $h, $m, $s
     if $h < 24;
  }
  else {
    if ( $id == 2 ) {
      return sprintf "Time between Zones\t%.3f\t\tss\n", $s if $s < 60;
      my $m = $s / 60;
      $s = $s % 60;
      return sprintf "Time between Zones\t%02d:%02d\t\tmm:ss\n", $m, $s
       if $m < 60;
      my $h = $m / 60;
      $m %= 60;
      return sprintf "Time between Zones\t%02d:%02d:%02d\thh:mm:ss\n", $h, $m, $s
       if $h < 24;
    }
    else {
      return "Invalid Time format";
    }
  }
}

sub csv {                                                                                                              # Write csv file for all events
  my $outfile = "$datestring" . "_Zonechange" . ".csv" or die "$!";
  unconditionaldelete($outfile);                                                                                       # Unconditional delete of file if exits
  open OUT_F, '>>', $outfile;                                                                                          # Open FileHandle for writing
  my @header = (
                 "Batchid,",                 "First_lift,",
                 "Last_lift,",               "Total_lifts,",
                 "Batchtime,",               "Time_to_next_batch,",
                 "First_id_next_batch,",     "First_shid_next_batch,",
                 "TrolleyEnteredJunction,",  "TrolleyLeftJunction,",
                 "TrolleyEnteredWorkspace,", "LiftUnloaded,LiftDetectedAtBcr3\n"
               );
  foreach my $header (@header) {
    print OUT_F $header;
  }

  foreach my $batchid ( keys %hoh ) {
    if ( $batchid ne "0" ) {
      my $row;
      my $first_lift            = $hoh{$batchid}{"First Lift"};
      my $last_lift             = $hoh{$batchid}{"Last Lift"};
      my $total_lifts           = $hoh{$batchid}{"Total Lifts"};
      my $batchtime             = $hoh{$batchid}{"Diff1"};
      my $time_to_next_batch    = $hoh{$batchid}{"Diff2"};
      my $first_id_next_batch   = $hoh{$batchid}{"ID"};
      my $first_shid_next_batch = $hoh{$batchid}{"SHID"};
      my $entered_jun           = $hoh{$batchid}{"Event_TrolleyEnteredJunction"};
      my $left_jun              = $hoh{$batchid}{"Event_TrolleyLeftJunction"};
      my $entered_workspace     = $hoh{$batchid}{"Event_TrolleyEnteredWorkspace"};
      my $unloaded              = $hoh{$batchid}{"Event_LiftUnloaded"};
      my $bcr3                  = $hoh{$batchid}{"Event_LiftDetectedAtBcr3"};

      if ( defined($time_to_next_batch) ) {
        if ( defined($first_id_next_batch) ) {
          if ( defined($first_shid_next_batch) ) {
            if ( defined($entered_jun) ) {
              if ( defined($left_jun) ) {
                if ( defined($entered_workspace) ) {
                  if ( defined($unloaded) ) {
                    if ( defined($bcr3) ) {
                      my @row = (
                                  $batchid,     $first_lift,         $last_lift,           $total_lifts,
                                  $batchtime,   $time_to_next_batch, $first_id_next_batch, $first_shid_next_batch,
                                  $entered_jun, $left_jun,           $entered_workspace,   $unloaded,
                                  $bcr3
                                );
                      foreach my $column (@row) {
                        unless ( defined($column) ) {
                          say my $e = "Error - $row[$column] Not defined";
                          warning_error($e);
                        }
                        print OUT_F $column;
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      else {
        my @row = ( $batchid, $first_lift, $last_lift, $total_lifts, $batchtime );
        foreach my $column (@row) {
          unless ( defined($column) ) { say my $e = "Error - $row[$column] Not defined"; warning_error($e) }
          my $row = "$batchid,$first_lift,$last_lift,$total_lifts,$batchtime,,,\n";
          print OUT_F $row;
        }
      }
    }
  }
  close OUT_F;
}

sub unconditionaldelete {                                                                                              # Uncondtonal delete if file exists
  my ($outfile) = @_;
  opendir my $DH, $cwd
   or die "Could not open '$cwd' for reading: $!\n";                                                                   # Open DirHandle
  if ( -e $outfile ) { unlink($outfile) or die "$outfile: $!" }
  closedir $DH;
}

sub warning_error {
  my $outfile = "$datestring" . "_Warnings_and_erors" . ".txt" or die "$!";
  unconditionaldelete($outfile);                                                                                       # Unconditional delete of file if exits

  open OUT_F, '>>', $outfile;
  my $e = shift;

  #my $batchid = shift;
  say $e;
  print OUT_F $e;
  close OUT_F;
}

sub help {                                                                                                             # *** HELP ***
  print "\nUsage:\n"
   . basename($0)
   . " [regexpattern] [filepattern] [outputfile] [-s or -S]\n\n"
   . "*) One or two arguments needed\.\n"
   . "*) First argument is file_Date as regex, for file pattern to search\.\n"
   . "*) Second argument is TimeDiff to use for report evaluation \.\n"
   . "*) The file pattern is always case insensitive\.\n" . "\n"
   . "*) For example \"_zonechange.pl SYP-PLC_2020-01-15 3\"\.\n"
   . "  - to match all files between SYP-PLC_2020-01-15 & SYP-PLC_2020-01-18\.\n";
  exit;
}

Main: {
  %regex = (                                                                                                           # Define Filename & RegEx

             'Event TrolleyEnteredJunction'  => '^[^P]+PLC\d-JUN1-JUI\d,[,\s.\w\[\]()]+<404>,',
             'Event TrolleyLeftJunction'     => '^[^P]+PLC\d-JUN1,[,\s\.]+A.+<401>,',
             'Event TrolleyEnteredWorkspace' => '^[^P]+PLC\d-USA1,\W+NewAsEvent(?!R)[^<]+<8>',
             'LiftUnloaded'                  => '^[^P]+PLC\d-USA1,\W+NewAsEvent(?!R)[^<]+<13>',
             'LiftDetectedAtBcr3'            => '^[^P]+PLC\d-USA1,\W+NewAsEvent(?!R)[^<]+<14>',
             'LiftPushedToBinder'            => '^[^P]+PLC\d-USA1,\W+NewAsEvent(?!R)[^<]+<15>',
           );

  if ( $#ARGV == 1 ) {                                                                                                 # Two input arguments, Datestring + Timediff as floating point
    ( $datestring, $max_time_diff ) = ( $ARGV[0], $ARGV[1] );
    $in_files = qr(SYF-PLC[68]_$datestring);
  }
  elsif ( $#ARGV == 0 ) {                                                                                              # One input arguments Datestring
    ($datestring) = ( $ARGV[0] );
    $in_files = qr(SYF-PLC[68]_$datestring);
  }
  else {
    help();
  }

  build_regex();
  find( \&findfiles, $cwd );
  if ( defined $match ) {
    write_files();
    build_hash();
    timediff();                                                                                                        # Do calculations
    find_shid();
    find_events();
    report();
    csv();

    #   say Dumper ( \%hoh );
  }
  else {
    say $in_files;
    say "NO SUCH FILE!!";
  }
}
