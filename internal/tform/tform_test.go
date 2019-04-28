package tform

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
)

type testPair struct {
	in    string
	out   string
	isErr bool
}

func checkTforms(t *testing.T, cases []testPair, tform func(string) (string, error)) {
	for _, test := range cases {
		out, err := tform(test.in)

		if test.isErr {
			if err == nil {
				t.Error(
					"For", test.in,
					"expected error",
					"got", out,
				)
			}
		} else {
			if err != nil {
				t.Error(
					"For", test.in,
					"expected", test.out,
					"got error", err,
				)
			} else if out != test.out {
				t.Error(
					"For", test.in,
					"expected", test.out,
					"got", out,
				)
			}
		}
	}
}

func TestParseDate(t *testing.T) {
	cases := []testPair{
		{"3/1/16", "2016-03-01", false},
		{"03/01/16", "2016-03-01", false},
		{"11/11/11", "2011-11-11", false},
		{"asdfasdf", "", true},
	}

	checkTforms(t, cases, parseDate)
}

func TestParsePartNumber(t *testing.T) {
	cases := []testPair{
		{"ABCD-EFG", "ABCDEFG", false},
		{"ABCD EFG", "ABCDEFG", false},
		{"ABCD.EFG", "ABCDEFG", false},
	}

	checkTforms(t, cases, parsePartNumber)
}

func TestParseQty(t *testing.T) {
	cases := []testPair{
		{"20", "20", false},
		{"2,000", "2000", false},
		{"2,000,000", "2000000", false},
		{"asdfasdf", "", true},
	}

	checkTforms(t, cases, parseQty)
}

func TestParseCurrency(t *testing.T) {
	cases := []testPair{
		{"1.00", "1.00", false},
		{"1,000.00", "1000.00", false},
		{"asdfasdf", "", true},
	}

	checkTforms(t, cases, parseCurrency)
}

func TestParsePercent(t *testing.T) {
	cases := []testPair{
		{"3%", "0.030", false},
		{"100%", "1.000", false},
		{"asdfasdf", "", true},
	}

	checkTforms(t, cases, parsePercent)
}

func TestDoTransform(t *testing.T) {
	inLine := `ITT Cannon,MCL Industries,Pulaski,WI,John Ivey,PEI - Genesis,,2/5/16,CA3106F181PF80,5,$15.22,$76.10,100%,$76.10,3%,$2.44,5/31/16,5/31/16,Major Account T2,Industrial,54162`

	outLine := `principal_division,end_customer_division,customer_city,customer_state,end_customer_acct_mgr,distributor,cem,invoice_date,mfr_part_number,qty_shipped,unit_price,billing_total,ext_split,split_total,comm_rate,comm_paid,comm_paid_date,comm_earned_date,category,cust_industry,cust_zip,mfr_part_number_strip
ITT Cannon,MCL Industries,Pulaski,WI,John Ivey,PEI - Genesis,,2016-02-05,CA3106F181PF80,5,15.22,76.10,1.000,76.10,0.030,2.44,2016-05-31,2016-05-31,Major Account T2,Industrial,54162,CA3106F181PF80
`

	reader := csv.NewReader(strings.NewReader(inLine))

	outBuf := new(bytes.Buffer)
	writer := csv.NewWriter(outBuf)

	err := DoTransform(reader, writer, 0)
	if err != nil {
		t.Error("Transform failed with error", err)
	} else if outBuf.String() != outLine {
		t.Error(
			"Transform expeceted:\n", outLine, "\n",
			"got:\n", outBuf.String(),
		)
	}
}
