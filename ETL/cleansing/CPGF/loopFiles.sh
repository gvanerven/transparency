#!/bin/bash

for inputFile in *.zip; 
do 
	./convertFilesCPGF.sh $inputFile; 
done
