#!/usr/bin/perl
my $Query=shift(@ARGV);
my $InputFile1=shift(@ARGV);
my $InputFile2=shift(@ARGV);
my $assume_header=1;
my $OutputLog="analysis_".$Query.".log";
my @ColPos;
my @ColF1;
my @ColF2;
my @ColStartF1;
my @ColEndF1;
my @ColStartF2;
my @ColEndF2;
my $NumColF1;
my $NumColF2;
my $QueryColPrecisionFile="colprecision.txt";
my @ColStart;
my @ColEnd;
my $NumMismatch=0;
open my $file1 , $InputFile1;
open my $file2 , $InputFile2;
$debug = 0;
# function to remove whitespace from the start and end of the string

# function to get next valid line
sub NextLine($)
{
  $filehandler = shift;
  my $line; 
  while ($line=<$filehandler>){
    chop($line);
    if (length($line)>0) {
      return $line;
    }  
  }
  return undef;
}

sub rtrim($)
{
  my $string = shift;
  #$string =~ s/^\s+//;
  $string =~ s/\s+$//;
  return $string;
}


# main

open my $file1 , $InputFile1;
open my $file2 , $InputFile2;
open(LOG,">$OutputLog");
open(QColPrecision,$QueryColPrecisionFile);

#get column precision information
$i=1;
while (($l=<QColPrecision>) && ($i<$Query)){
  $i++;
}
if ($i != $Query) {
  print "Could not find column precision for Query $Query\n";
} 
@QueryColPrecision=split(/\s/,$l);

#print @QueryColPrecision;
$NumRowsF1=0;
$NumRowsF2=0;
if ($assume_header==1){
  $lf1=&NextLine($file1); 
  $lf2=&NextLine($file2); 
}
while (1){
  $lf1=&NextLine($file1);
  #print "File 1 $lf1\n";
  $lf2=&NextLine($file2);
  #print "File 2 $lf2\n";
  if ((!defined($lf1)) && (defined($lf2))) {
    print LOG "File1 ($InputFile1) exhausted before File2 ($InputFile2)\n";
    print LOG "Number of rows processed $NumRowsF1\n";
    exit;
  } elsif ((defined($lf1)) && (!defined($lf2))) {
         print LOG "File2 ($InputFile2) exhausted before File1 ($InputFile1)\n";
         print LOG "Number of rows processed $NumRowsF1\n";
         exit;
       } elsif ((!defined($lf1)) && (!defined($lf2))) {
         #done
         print LOG "Found $NumMismatch unacceptable missmatches\n";
         print "Query $Query $NumMismatch unacceptable missmatches\n";
         exit;
         }
  $NumRowsF1++;
  $NumRowsF2++;
  #print "Comparing row $lf1 with $lf2";
  # The following split implements fixed column length 
  #print "Splitting row $NumColF1: $lf1\n";
  @ColF1=split(/\|/,$lf1);
  $NumColF1=@ColF1;
  #print "Splitting row $NumColF2: $lf2\n";
  @ColF2=split(/\|/,$lf2);
  $NumColF2=@ColF2;
  # as needed we can extend to other splits
  if ($NumColF1!=$NumColF2) {
    print LOG "Number of column mismatch in row $NumRowsF1\n";
    exit;
  }
  #print $NumColF1."|".$NumColF2."\n";
  for ($col=0;$col<$NumColF1;$col++){
    #print "@ColF1[$col]| ";
    @ColF1[$col]=&rtrim(@ColF1[$col]);  
    #print "@ColF1[$col]|\n";  
    #print "@ColF2[$col]|";
    @ColF2[$col]=&rtrim(@ColF2[$col]);
    #print "@ColF2[$col]|\n";
  }
  # comparison
  for ($col=0;$col<$NumColF1;$col++){
    #print "Comparing @ColF1[$col] and @ColF2[$col]\n";
    $difference=0; 
    $mismatch=0; 
    #print "@QueryColPrecision[$col]\n";
    for (@QueryColPrecision[$col]) {
        if    (/str/)  {$mismatch = @ColF1[$col] ne @ColF2[$col] ? 1 : 0}     #column is string and needs to match exactly
        elsif (/sum/)  {@ColF1[$col]=sprintf("%.2f", @ColF1[$col]);
                        @ColF2[$col]=sprintf("%.2f", @ColF2[$col]);
                        $difference = abs(@ColF1[$col] - @ColF2[$col]);
                        $pdifference= sprintf("%.4f",(($difference/@ColF1[$col])*100));
                        $mismatch = $difference>100 ? 1 : 0;
                        $pdifference= sprintf("%.4f",($difference/@ColF1[$col])*100); }     #column is sum and needs to be with 100 
        elsif (/avg/)  {@ColF1[$col]=sprintf("%.2f", @ColF1[$col]);
                        @ColF2[$col]=sprintf("%.2f", @ColF2[$col]);
                        $difference = abs(@ColF1[$col] - @ColF2[$col]);
                        $pdifference= sprintf("%.4f",(($difference/@ColF1[$col])*100));
                        $mismatch = $pdifference>1 ? 1 : 0}    #column is avg and needs to be within 1 percent when reported to the nearest 1/100th, rounded up
        elsif (/cnt/)  {$mismatch=1 if (@ColF1[$col] != @ColF2[$col])}    #column is cnt and needs to match exactly
        elsif (/int/)  {$mismatch=1 if (@ColF1[$col] != @ColF2[$col])}    #column is int and needs to match exactly
        elsif (/num/)  {@ColF1[$col]=sprintf("%.2f", @ColF1[$col]);
                        @ColF2[$col]=sprintf("%.2f", @ColF2[$col]);
                        $mismatch=1 if (@ColF1[$col] != @ColF2[$col]);
                        $difference = abs(@ColF1[$col] - @ColF2[$col]);
                        $pdifference= sprintf("%.4f",(($difference/@ColF1[$col])*100));}    #column is num and needs to match exactly when reported to the nearest 1/100th, rounded up
        elsif (/rat/)  {@ColF1[$col]=sprintf("%.2f", @ColF1[$col]); 
                        @ColF2[$col]=sprintf("%.2f", @ColF2[$col]);
                        $difference = abs(@ColF1[$col] - @ColF2[$col]); 
                        $pdifference= sprintf("%.4f",(($difference/@ColF1[$col])*100)); 
                        $mismatch = $difference > 1 ? 1 : 0;} #column is a ratio and needs to match within 1 percent when reported to the nearest 1/100th, rounded up
        else           {print LOG "Don't know how to compare type @QueryColPrecision[$col]\n";exit;}     # default
    }
    #print "$mismatch $difference\n"; 
    if ($mismatch == 1) {
      $NumMismatch++;
      if (@QueryColPrecision[$col]=~"str"){
        printf( LOG "%s\n%30s%s\n%30s%s\n", "Difference in row $NumRowsF1 column $col using @QueryColPrecision[$col] ----------------> VIOLATION OF SPEC","$InputFile1:","@ColF1[$col]","$InputFile2:","@ColF2[$col]");}
      else {
      print LOG "Difference in row $NumRowsF1 column $col @ColF1[$col] @ColF2[$col] using @QueryColPrecision[$col]: total diff $difference percent diff $pdifference"." % ----------------> VIOLATION OF SPEC\n";}
    } elsif ($difference>0) {
           print LOG "Difference in row $NumRowsF1 column $col F1=@ColF1[$col] F2=@ColF2[$col] using @QueryColPrecision[$col] total diff $difference percent diff $pdifference ----------------> OK WITH SPEC\n";
    }
  }
}
