package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

type DbWorker struct {
	Dsn string
	Db  *sql.DB
}

type JdpTbRefund struct {
	RefundId    int
	SellerNick  string
	BuyerNick   string
	Status      string
	Created     string
	Tid         int
	Oid         int
	Modified    string
	JdpHashcode string
	JdpResponse string
	JdpCreated  string
	JdpModified string
}

func (dbw *DbWorker) queryData(ids int) {
	fmt.Printf("ids:", ids)
	stmt, _ := dbw.Db.Prepare(`select * from jdp_tb_refund where refund_id=?`)
	defer stmt.Close()
	jtr := JdpTbRefund{}

	rows, err := stmt.Query(ids)
	defer rows.Close()

	if err != nil {
		log.Fatal("quert data error %v\n", err)
		return
	}

	for rows.Next() {
		rows.Scan(&jtr.RefundId, &jtr.SellerNick, &jtr.BuyerNick, &jtr.Status, &jtr.Created, &jtr.Tid, &jtr.Oid,
			&jtr.Modified, &jtr.JdpHashcode, &jtr.JdpResponse, &jtr.JdpCreated, &jtr.JdpModified)
		if err != nil {
			log.Fatal(err.Error())
			continue
		}
		fmt.Print("get data, JdpResponse:", jtr.JdpResponse)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err.Error())
	}
}

func writeToCsv(file string, columns []string, totalValues [][]string) {
	f, err := os.Create(file)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	w := csv.NewWriter(f)
	for i, row := range totalValues {
		if i == 0 {
			w.Write(columns)
			w.Write(row)
		} else {
			w.Write(row)
		}
	}
	w.Flush()
	fmt.Println("完成：", file)

}

func main() {
	//var err error
	//dbw := DbWorker{
	//	Dsn: "cy:Cy_2016_@tcp(192.168.142.128:3313)/sys_info?charset=utf8",
	//}
	//dbw.Db, err = sql.Open("mysql", dbw.Dsn)
	//defer dbw.Db.Close()
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}

	//dbw.queryData(16778848088051807)

	//rows, err := mysqldb.Query("select * from jdp_tb_refund")

	db, err := sql.Open("odbc",
		"Driver={vertica};ServerName=106.15.24.126;DataBase=hmcdata;UID=hmc;PWD=Inman2018;Port=5433;")
	f, err := os.Create("test.csv")
	defer f.Close()
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}

	db.Ping()
	rows, err := db.Query("select Id, order_goods_Id, order_id, goods_name, goods_id, goods_sn from  hmcdata.e3_order_goods limit 100000")
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}
	var (
		id         int
		color_id   int
		color_code string
		color_name string
	//color_note       string
	//outer_color_code string
	//lastchanged      string
	//gmt_src_created  string
	//gmt_created      string
	//gmt_modified     string
	//src_business_id  string
	)
	//fmt.Print(rows)
	columns, err := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanAargs := make([]interface{}, len(values))

	for i := range values {
		scanAargs[i] = &values[i]
	}

	totalValues := make([][]string, 0)

	for rows.Next() {
		var s []string
		err := rows.Scan(scanAargs...)
		if err != nil {
			panic(err.Error())
		}

		for _, v := range values {
			s = append(s, string(v))
		}
		totalValues = append(totalValues, s)

		if err = rows.Err(); err != nil {
			panic(err.Error())
		}
		//writeToCsv("test.csv", columns, totalValues)

		fmt.Printf("id is %d, color id is %d, code is %s, name is %s. \n", id, color_id, color_code, color_name) //id, color_id, color_code, color_name
	}

	return
}
