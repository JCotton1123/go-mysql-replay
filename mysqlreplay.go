package main

import (
    "database/sql"
    "encoding/csv"
    "encoding/json"
    "flag"
    "fmt"
    "github.com/go-sql-driver/mysql"
    "io"
    "math"
    "os"
    "strconv"
    "sync"
    "time"
)

type Configuration struct {
    Dsn string
}

type SqlCommand struct {
    sessionid int
    epoch     float64
    stmt      string
    cmd       uint8
}

func timefromfloat(epoch float64) time.Time {
    epoch_base := math.Floor(epoch)
    epoch_frac := epoch - epoch_base
    epoch_time := time.Unix(int64(epoch_base), int64(epoch_frac*1000000000))
    return epoch_time
}

func parsefields(line []string) (SqlCommand, error) {
    if len(line) != 4 {
        return SqlCommand{}, fmt.Errorf("Invalid data line. Length = %d", len(line))
    }

    sessionid, err := strconv.Atoi(line[0])
    if err != nil {
        return SqlCommand{}, fmt.Errorf("Failed to parse session id with err: %s", err)
    }

    epoch, err := strconv.ParseFloat(line[1], 64)
    if err != nil {
        return SqlCommand{}, fmt.Errorf("Failed to parse epoch with err: %s", err)
    }

    cmd_src, err := strconv.Atoi(line[2])
    if err != nil {
        return SqlCommand{}, fmt.Errorf("Failed to parse cmd with err: %s", err)
    }
    cmd := uint8(cmd_src)

    var stmt string = line[3]

    return SqlCommand{sessionid: sessionid, epoch: epoch, cmd: cmd, stmt: stmt}, nil
}

func main() {
    conffile, _ := os.Open("go-mysql-replay.conf.json")
    confdec := json.NewDecoder(conffile)
    config := Configuration{}
    err := confdec.Decode(&config)
    if err != nil {
        fmt.Printf("[main] Error reading configuration from "+
            "'./go-mysql-replay.conf.json': %s\n", err)
    }

    fileflag := flag.String("f", "./test.dat", "Path to datafile for replay")
    flag.Parse()

    datFile, err := os.Open(*fileflag)
    if err != nil {
        panic(err.Error())
    }

    var line_num uint32 = 0
    reader := csv.NewReader(datFile)
    reader.Comma = '\t'

    db, err := sql.Open("mysql", config.Dsn)
    if err != nil {
        panic(err.Error())
    }
    db.SetMaxIdleConns(100)

    var wg sync.WaitGroup

    var firstepoch float64 = 0.0
    starttime := time.Now()
    var last_stmt_epoch = 0.0

    for {
        line, err := reader.Read()
        if err == io.EOF {
            break
        }
        line_num++

        sqlcmd, err := parsefields(line)
        if err != nil {
            fmt.Println("[main] Error parsing line", line_num, "with error", err)
            continue
        }

        if firstepoch == 0.0 {
            firstepoch = sqlcmd.epoch
        } else if last_stmt_epoch == sqlcmd.epoch {
            //No sleeping
        } else {
            firsttime := timefromfloat(firstepoch)
            pkttime := timefromfloat(sqlcmd.epoch)
            delaytime_orig := pkttime.Sub(firsttime)
            mydelay := time.Since(starttime)
            delaytime_new := delaytime_orig - mydelay

            fmt.Printf("[main] Sleeptime: %s\n", delaytime_new)
            time.Sleep(delaytime_new)
        }
        last_stmt_epoch = sqlcmd.epoch
        
        

        fmt.Println("[main] Worker for ", line)
        wg.Add(1)
        go func(cmd SqlCommand) {
            sessionid := cmd.sessionid

            switch cmd.cmd {
                    case 3: // Query
                        fmt.Printf("[session %d] Replay: %s\n", sessionid, cmd.stmt)
                        _, err := db.Exec(cmd.stmt)
                        if err != nil {
                            if mysqlError, ok := err.(*mysql.MySQLError); ok {
                                if mysqlError.Number == 1205 { // Lock wait timeout
                                    fmt.Printf("ERROR IGNORED: %s",
                                        err.Error())
                                }
                            } else {
                                panic(err.Error())
                            }
                        }
           }

           defer wg.Done()

        }(sqlcmd)
    }
    fmt.Println("[main] Waiting for children to complete.")
    wg.Wait()
}
