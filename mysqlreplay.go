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
        fmt.Println(err)
    }

    reader := csv.NewReader(datFile)
    reader.Comma = '\t'

    var wg sync.WaitGroup
    sessions := make(map[int]int)

    var firstepoch float64 = 0.0
    starttime := time.Now()

    for {
        line, err := reader.Read()
        if err == io.EOF {
            break
        }
        
        scmd, err := parsefields(line)
        if err != nil {
            fmt.Println("[main] Error: ", err, ". Skipping line: ", line)
            continue
        }
        sessionid := scmd.sessionid

        if firstepoch == 0.0 {
            firstepoch = scmd.epoch
        }

        if sessions[sessionid] == 0 {
            sessions[sessionid] = 1

            wg.Add(1)
            go func(rsessionid int) {
                fmt.Printf("[session %d] New session\n", rsessionid)

                rDatFile, err := os.Open(*fileflag)
                if err != nil {
                    fmt.Printf("[session %d] Failed to open data file with error %s.\n", rsessionid, err)
                }

                rReader := csv.NewReader(rDatFile)
                rReader.Comma = '\t'

                db, err := sql.Open("mysql", config.Dsn)
                if err != nil {
                    panic(err.Error())
                }
                dbopen := true
                defer db.Close()

                last_stmt_epoch := firstepoch

                for {
                    rLine, err := rReader.Read()
                    if err == io.EOF {
                        break
                    }

                    sqlcmd, err := parsefields(rLine)
                    if err != nil {
                        fmt.Printf("[session %d] Failed to parse line %s with err %s.\n", rsessionid, rLine, err)
                        continue
                    }

                    if sqlcmd.sessionid != rsessionid {
                        continue
                    }

                    if last_stmt_epoch != 0.0 {
                        firsttime := timefromfloat(firstepoch)
                        pkttime := timefromfloat(sqlcmd.epoch)
                        delaytime_orig := pkttime.Sub(firsttime)
                        mydelay := time.Since(starttime)
                        delaytime_new := delaytime_orig - mydelay

                        fmt.Printf("[session %d] Sleeptime: %s\n", rsessionid, delaytime_new)
                        time.Sleep(delaytime_new)
                    }
                    last_stmt_epoch = sqlcmd.epoch

                    switch sqlcmd.cmd {
                    case 14: // Ping
                        continue
                    case 1: // Quit
                        fmt.Printf("[session %d] COMMAND REPLAY: QUIT\n", rsessionid)
                        dbopen = false
                        db.Close()
                    case 3: // Query
                        if dbopen == false {
                            fmt.Printf("[session %d] RECONNECT\n", rsessionid)
                            db, err = sql.Open("mysql", config.Dsn)
                            if err != nil {
                                panic(err.Error())
                            }
                            dbopen = true
                        }
                        fmt.Printf("[session %d] STATEMENT REPLAY: %s\n", rsessionid, sqlcmd.stmt)
                        _, err := db.Exec(sqlcmd.stmt)
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
                }

                fmt.Printf("[session %d] Session complete\n", rsessionid)
                defer wg.Done()

            }(sessionid)
        }
    }
    fmt.Println("[main] Waiting for children to complete.")
    wg.Wait()
}
