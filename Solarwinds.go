package main

import (
  "database/sql"
  _ "github.com/denisenkom/go-mssqldb"
  "github.com/influxdata/influxdb/client/v2"
  "log"
  "os"
  "fmt"
  "time"
  "encoding/json"
  "io/ioutil"
  )

const layout = "20060102"
var (
  in_mbps, out_mbps int	)
  
type Network []struct {
	Site  string `json:"site"`
	Links []struct {
		Device string `json:"device"`
		Port   string `json:"port"`
		Desc   string `json:"desc"`
	} `json:"links"`
}

func influxDBClient() client.Client {
  c, err := client.NewHTTPClient(client.HTTPConfig{
    Addr: "https://<<host>>:8086",
    Username: "XXXX",
    Password: "XXXX",
    InsecureSkipVerify: true,
  })
  if err!= nil {
    log.Fatalln("Error: ",err)
  }
  return c
}

func main() {
  t := time.Now()
  log.SetOutput(os.Stdout)
  inf := influxDBClient()
  bp, err := client.NewBatchPoints(client.BatchPointsConfig{
    Database: "network_capacity",
    Precision: "s",
  })
  
  raw, err := ioutil.ReadFile("/root/go/network_ext_links.json")
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
  c := Network{}
  if err := json.Unmarshal(raw, &c); err != nil {
    fmt.Println(err.Error())
    os.Exit(1)
    }
 
  conn, err := sql.Open("mssql", "server=<<host>>\\<<DBName>>;user id=<<user>>;password=<<pwd>>")
  if err != nil {
    log.Fatalln("Open connection failed:", err.Error())
  }
  defer conn.Close()
  for _, site := range c {
  	for _, link := range site.Links {
  		query := fmt.Sprintf(`SELECT CEILING(MAX(i.In_Maxbps/(1000*1000))) as 'Max_In_Mbps', 
  		CEILING(MAX(i.Out_Maxbps/(1000*1000))) as 'Max_Out_Mbps'
  FROM [SolarWindsOrionNPM].[dbo].[InterfaceTraffic_Detail_%s] i,
   [SolarWindsOrionNPM].[dbo].[Nodes] n, 
   [SolarWindsOrionNPM].[dbo].[Interfaces]id
  WHERE
  id.InterfaceName = '%s'
  AND i.NodeID = n.NodeID
  AND n.NodeID = id.NodeID
  AND i.InterfaceID = id.InterfaceID
  AND n.SysName ='%s'`,t.Format(layout),link.Port, link.Device  )
  //fmt.Println(query)
    stmt, err := conn.Prepare(query)
  if err != nil {
     log.Fatal("Prepare failed:", err.Error())
     return
	}
	defer stmt.Close()
	
	rows, err := stmt.Query()
	if err != nil {
		log.Fatal("Query failed:", err.Error())
		return
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&in_mbps, &out_mbps)
		if err != nil {
        		log.Fatal(err)
			 }
		tags := map[string]string{
			"site": site.Site,
			"device": link.Device,
			"port": link.Port,
			"description": link.Desc,
		}
		
		fields := map[string]interface{}{
			"max_in_mbps": in_mbps,
			"max_out_mbps": out_mbps,
		}
	point, err := client.NewPoint(
        fmt.Sprintf("internet_capacity",),
        tags,
        fields,
        t.AddDate(0, 0, -1).UTC(),
        )
      if err != nil {
                log.Fatalln("Error: ", err)
            }
        bp.AddPoint(point)
	}
	err = inf.Write(bp)
  	if err != nil {
    		log.Fatal(err)
  		}
  	if err = rows.Err(); err != nil {
    		log.Fatal(err)
		}
  }
 }

}
