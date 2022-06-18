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

const None string = "None"

const dateForm string = "2006-01-02"

const csv_file string = "data.csv"

type Converter interface {
	Convert(arg string)
}

func Parser(arg string, v Converter) {
	v.Convert(arg)
}

type date time.Time

func (d date) String() string {
	nt := time.Time(d)
	if nt.IsZero() {
		return None
	}
	return fmt.Sprintf("%v", nt.Format(dateForm))
}

func (d *date) Convert(arg string) {
	if arg == "" {
		*d = date(time.Time{})
	} else {
		t, err := time.Parse(dateForm, arg)
		if err != nil {
			log.Print(err)
			*d = date(time.Time{})
		}
		*d = date(t)
	}
}

type integer int

func (i *integer) Convert(arg string) {
	n, err := strconv.Atoi(arg)
	if err != nil {
		log.Print(err)
		*i = 0
	}
	*i = integer(n)
}

type amount float64

func (f *amount) Convert(arg string) {
	s, err := strconv.ParseFloat(arg, 32)
	if err != nil {
		log.Print(err)
		*f = 0.0
	}
	*f = amount(s)
}

type str string

func (s *str) Convert(arg string) {
	*s = str(arg)
}

// names = ['CUST_ID', 'START_DATE', 'END_DATE', 'TRANS_ID', 'DATE', 'YEAR',
// 'MONTH', 'DAY', 'EXP_TYPE', 'AMOUNT']

type record struct {
	cust_id    str
	start_date date
	end_date   date
	trans_id   str
	date       date
	year       integer
	mounth     integer
	day        integer
	exp_type   str
	amount     amount
}

func parseRecord(args []string) *record {
	var cust_id str
	Parser(args[0], &cust_id)

	var start_date date
	Parser(args[1], &start_date)

	var end_date date
	Parser(args[2], &end_date)

	var trans_id str
	Parser(args[3], &trans_id)

	var date date
	Parser(args[4], &date)

	var year integer
	Parser(args[5], &year)

	var mounth integer
	Parser(args[6], &mounth)

	var day integer
	Parser(args[7], &day)

	var exp_type str
	Parser(args[8], &exp_type)

	var amount amount
	Parser(args[9], &amount)

	r := record{
		cust_id:    cust_id,
		start_date: start_date,
		end_date:   end_date,
		trans_id:   trans_id,
		date:       date,
		year:       year,
		mounth:     mounth,
		day:        day,
		exp_type:   exp_type,
		amount:     amount,
	}
	return &r
}

func (r record) String() string {
	return fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %v, %.2f",
		r.cust_id, r.start_date, r.end_date, r.trans_id, r.date, r.year, r.mounth, r.day, r.exp_type, r.amount)
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
		fmt.Println(out)
	}
}
