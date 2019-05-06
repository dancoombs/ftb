package main

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"

	"github.com/dancoombs/ftb/internal/awsjson"
)

var query = `SELECT *
FROM (
    SELECT DISTINCT d.*, res.total_billing_to_date
    FROM (
        SELECT l.*, f.total_billing_to_date
        FROM (
            SELECT principal_division AS principal,
                end_customer_division AS cust,
                mfr_part_number_strip AS pn,
                min(cast(invoice_date AS DATE)) AS date,
                sum(cast(billing_total AS DOUBLE)) AS total_billing_to_date
            FROM default.ftb
            WHERE cast(invoice_date as DATE) >= date 'FILTER_DATE'
            GROUP BY  principal_division, end_customer_division, mfr_part_number_strip
            HAVING sum(cast(billing_total AS DOUBLE)) > 500.0
        ) AS f
        JOIN (
            SELECT principal_division, end_customer_division, invoice_date, mfr_part_number_strip, commission_earned_date, max(cast(billing_total AS DOUBLE)) as max_bill
            FROM default.ftb
            WHERE cast(commission_earned_date as DATE) = date 'COMM_DATE'
            GROUP BY principal_division, end_customer_division, invoice_date, mfr_part_number_strip, commission_earned_date
        ) AS l
        ON l.principal_division = f.principal
        AND l.end_customer_division = f.cust
        AND l.mfr_part_number_strip = f.pn
        AND cast(l.invoice_date AS DATE) = f.date
    ) as res
    JOIN default.ftb AS d
    ON res.principal_division = d.principal_division
    AND res.end_customer_division = d.end_customer_division
    AND res.invoice_date = d.invoice_date
    AND res.mfr_part_number_strip = d.mfr_part_number_strip
    AND res.commission_earned_date = d.commission_earned_date
    AND res.max_bill = cast(d.billing_total AS DOUBLE)
) as final
ORDER BY final.principal_division, final.end_customer_division, final.mfr_part_number_strip`

func handleRequest(ctx context.Context, createEvent awsjson.S3Event) (string, error) {
	inKey := createEvent.Records[0].S3.Object.Key
	log.Println("Running ftb query for infile:", inKey)

	inDate := strings.TrimSuffix(inKey, filepath.Ext(inKey))

	t, err := time.Parse("2006-01-02", inDate)
	if err != nil {
		return "Failed to parse date", errors.New("Failed to parse date")
	}
	endDate := t.AddDate(-2, 0, 1)
	endDateStr := endDate.Format("2006-01-02")

	log.Println("Running query with commission date:", inDate, "and end date:", endDateStr)
	runQuery := strings.Replace(query, "COMM_DATE", inDate, 1)
	runQuery = strings.Replace(runQuery, "FILTER_DATE", endDateStr, 1)

	log.Println("Running query:", runQuery)

	sess := session.Must(session.NewSession())
	svc := athena.New(sess)

	startOutput, err := svc.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryString: &runQuery,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://ftb-results/"),
		},
	})
	if err != nil {
		return "Failure to start execution of named query", err
	}
	log.Println("Submitted query:", startOutput)

	for {
		execRes, err := svc.GetQueryExecution(&athena.GetQueryExecutionInput{
			QueryExecutionId: startOutput.QueryExecutionId,
		})
		if err != nil {
			return "Failure to get query execution status", err
		}

		switch *execRes.QueryExecution.Status.State {
		case "QUEUED":
			time.Sleep(time.Duration(time.Millisecond * 500))
		case "RUNNING":
			time.Sleep(time.Duration(time.Millisecond * 500))
		case "SUCCEEDED":
			return "Success", nil
		case "FAILED":
			log.Println("Query failed with status:", execRes.QueryExecution.Status)
			return "Query failed", errors.New("Query execution failed")
		case "CANCELED":
			log.Println("Query failed with status:", execRes.QueryExecution.Status)
			return "Query failed", errors.New("Query execution was canceled")
		default:
			log.Println("Unexpected query status:", execRes.QueryExecution.Status)
			return "Unexpected query status", errors.New("Unexpected query status")
		}
	}
}

func main() {
	lambda.Start(handleRequest)
}
