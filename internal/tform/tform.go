package tform

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func parseDate(in string) (string, error) {
	t, err := time.Parse("1/2/06", in)
	if err != nil {
		return "", err
	}
	return t.Format("2006-01-02"), nil
}

func parsePartNumber(in string) (string, error) {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		panic(err)
	}
	return reg.ReplaceAllString(in, ""), nil
}

func parseQty(in string) (string, error) {
	out := strings.ReplaceAll(in, ",", "")
	if _, err := strconv.ParseFloat(out, 32); err != nil {
		return "", err
	}
	return out, nil
}

func parseCurrency(in string) (string, error) {
	return parseQty(strings.ReplaceAll(in, "$", ""))
}

func parsePercent(in string) (string, error) {
	p, err := strconv.ParseFloat(strings.ReplaceAll(in, "%", ""), 32)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%.3f", p/100.0), nil
}

type tform struct {
	name  string
	inCol int
	run   func(string) (string, error)
}

var tforms = [...]tform{
	{"principal_division", 0, nil},
	{"end_customer_division", 1, nil},
	{"customer_city", 2, nil},
	{"customer_state", 3, nil},
	{"end_customer_acct_mgr", 4, nil},
	{"distributor", 5, nil},
	{"cem", 6, nil},
	{"invoice_date", 7, parseDate},
	{"mfr_part_number", 8, nil},
	{"qty_shipped", 9, parseQty},
	{"unit_price", 10, parseCurrency},
	{"billing_total", 11, parseCurrency},
	{"ext_split", 12, parsePercent},
	{"split_total", 13, parseCurrency},
	{"comm_rate", 14, parsePercent},
	{"comm_paid", 15, parseCurrency},
	{"comm_paid_date", 16, parseDate},
	{"comm_earned_date", 17, parseDate},
	{"category", 18, nil},
	{"cust_industry", 19, nil},
	{"cust_zip", 20, nil},
	{"mfr_part_number_strip", 8, parsePartNumber},
}

// DoTransform comment
func DoTransform(csvReader *csv.Reader, csvWriter *csv.Writer, firstLine int) error {
	// write the header
	header := make([]string, len(tforms))
	for i, tform := range tforms {
		header[i] = tform.name
	}
	err := csvWriter.Write(header)
	if err != nil {
		log.Println("Failed to write header to csv with error", err)
		return err
	}

	// perform the transforms
	lineNum := 0
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println("Failed to read line", lineNum, "from csv with error", err)
			return err
		}

		lineNum++
		if lineNum < (firstLine + 1) {
			continue
		}

		record := make([]string, len(tforms))
		isErr := false
		for i, tform := range tforms {
			if tform.run != nil {
				if line[tform.inCol] == "" {
					record[i] = ""
					continue
				}

				out, err := tform.run(line[tform.inCol])
				if err != nil {
					log.Println(
						"Failed to transform column", i,
						"of record", line[tform.inCol],
						"with error", err,
						"skipping record",
					)
					isErr = true
				} else {
					record[i] = out
				}
			} else {
				record[i] = line[tform.inCol]
			}

			if isErr {
				break
			}
		}

		if !isErr {
			err = csvWriter.Write(record)
			if err != nil {
				log.Println("Failed to write line", lineNum, "to csv with error", err)
				return err
			}
		}
	}

	csvWriter.Flush()
	err = csvWriter.Error()
	if err != nil {
		log.Println("Failed final flush of csv writer with error", err)
		return err
	}

	return nil
}
