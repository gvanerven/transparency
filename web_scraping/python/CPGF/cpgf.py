#!/usr/bin/env python3
import sys
import urllib.request

def main(argv):
    if len(argv) == 5:
        directoryBase = argv[1]
        baseName = argv[2]
        sYear = int(argv[3])
        eYear = int(argv[4]) + 1
        link = ''
        for year in range(sYear,eYear):
            for month in range(1,13):
                link = 'http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=' + str(year) + '&m=' + str(month).zfill(2) + '&consulta=CPGF'
                print("Saving " + directoryBase + baseName + str(year) + str(month).zfill(2) + ".zip")		
                urllib.request.urlretrieve(link, directoryBase + baseName + str(year) + str(month) + ".zip")
    else:
        print("Usage: " + argv[0] + " <directory path> <base name> <start year> <end year>")
    
if __name__ == "__main__":
    main(sys.argv)

