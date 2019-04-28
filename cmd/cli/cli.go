package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"

	"github.com/dancoombs/ftb/internal/tform"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	inFileName := flag.String("i", "", "File name to transform")
	outFileName := flag.String("o", "", "File name of output")
	flag.Parse()

	log.Println("Opening", *inFileName, "transforming to", *outFileName)
	inFile, err := os.Open(*inFileName)
	check(err)
	defer inFile.Close()

	outFile, err := os.Create(*outFileName)
	check(err)
	defer outFile.Close()

	reader := csv.NewReader(inFile)
	writer := csv.NewWriter(outFile)

	tform.DoTransform(reader, writer, 11)
}
