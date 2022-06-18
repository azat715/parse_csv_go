package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

const dateForm string = "2006-01-02"

const csv_file string = "data.csv"

func parseDate(arg string) time.Time {
	if arg == "" {
		return time.Time{}
	}
	t, err := time.Parse(dateForm, arg)
	if err != nil {
		log.Print(err)
		return time.Time{}
	}
	return t
}

func parseInt(arg string) int {
	i, err := strconv.Atoi(arg)
	if err != nil {
		log.Print(err)
		return 0
	}
	return i
}

func parseFloat(arg string) float64 {
	s, err := strconv.ParseFloat(arg, 32)
	if err != nil {
		log.Print(err)
		return 0
	}
	return s

}

// names = ['CUST_ID', 'START_DATE', 'END_DATE', 'TRANS_ID', 'DATE', 'YEAR',
// 'MONTH', 'DAY', 'EXP_TYPE', 'AMOUNT']

type record struct {
	cust_id    string
	start_date time.Time
	end_date   time.Time
	trans_id   string
	date       time.Time
	year       int
	mounth     int
	day        int
	exp_type   string
	amount     float64
}

func parseRecord(args []string) *record {
	r := record{
		cust_id:    args[0],
		start_date: parseDate(args[1]),
		end_date:   parseDate(args[2]),
		trans_id:   args[3],
		date:       parseDate(args[4]),
		year:       parseInt(args[5]),
		mounth:     parseInt(args[6]),
		day:        parseInt(args[7]),
		exp_type:   args[8],
		amount:     parseFloat(args[9]),
	}
	return &r

}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func parseFile(file string) <-chan []string {
	out := make(chan []string)

	f, err := os.Open(file)
	check(err)
	parser := csv.NewReader(f)
	go func() {
		for {
			record, err := parser.Read()
			if err == io.EOF {
				break
			}
			check(err)
			out <- record
		}
		close(out)
	}()
	return out
}

func mappingStruct(in <-chan []string) <-chan record {
	out := make(chan record)
	go func() {
		for record := range in {
			out <- *parseRecord(record)
		}
		close(out)
	}()
	return out
}

func main() {
	first := parseFile(csv_file)
	second := mappingStruct(first)

	for {
		out := <-second
		fmt.Print(out.start_date)
	}
}
