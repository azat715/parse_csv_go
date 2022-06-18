package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	None        string = "None"
	dateForm    string = "2006-01-02"
	csv_file    string = "data.csv"
	calculators int    = 10
)

type counter int32

func (c *counter) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}

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
	var res record
	Parser(args[0], &res.cust_id)
	Parser(args[1], &res.start_date)
	Parser(args[2], &res.end_date)
	Parser(args[3], &res.trans_id)
	Parser(args[4], &res.date)
	Parser(args[5], &res.year)
	Parser(args[6], &res.mounth)
	Parser(args[7], &res.day)
	Parser(args[8], &res.exp_type)
	Parser(args[9], &res.amount)
	return &res
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

	var c counter

	var wg sync.WaitGroup
	wg.Add(calculators)
	for i := 0; i < calculators; i++ {
		go func() {
			for range second {
				c.inc()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println(c)
}
