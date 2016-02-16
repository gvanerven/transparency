#!/bin/bash

fileName=$1
fileExtracted=`unzip -Z -1 $fileName`
unzip -o $fileName
sed 's/\x0/-/g' $fileExtracted > $fileExtracted".tmp"
iconv -f ISO8859-1 -t UTF-8 -o $fileExtracted $fileExtracted".tmp"
rm -f $fileExtracted".tmp"
dos2unix $fileExtracted
exit 0
