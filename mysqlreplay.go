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
    "time"
)

type Configuration struct {
    Dsn string
}

type ReplayStatement struct {
    session int
    epoch   float64
    stmt    string
    cmd     uint8
}

func timefromfloat(epoch float64) time.Time {
    epoch_base := math.Floor(epoch)
    epoch_frac := epoch - epoch_base
    epoch_time := time.Unix(int64(epoch_base), int64(epoch_frac*1000000000))
    return epoch_time
}

func parsefields(line []string) (ReplayStatement, error) {
    if len(line) != 4 {
        return ReplayStatement{}, fmt.Errorf("Invalid data line. Length = %d", len(line))
    }

    sessionid, err := strconv.Atoi(line[0])
    if err != nil {
        return ReplayStatement{}, fmt.Errorf("Failed to parse session id with err: %s", err)
    }

    epoch, err := strconv.ParseFloat(line[1], 64)
    if err != nil {
        return ReplayStatement{}, fmt.Errorf("Failed to parse epoch with err: %s", err)
    }

    cmd_src, err := strconv.Atoi(line[2])
    if err != nil {
        return ReplayStatement{}, fmt.Errorf("Failed to parse cmd with err: %s", err)
    }
    cmd := uint8(cmd_src)

    var stmt string = line[3]

    return ReplayStatement{session: sessionid, epoch: epoch, cmd: cmd, stmt: stmt}, nil
}

func mysqlsession(c <-chan ReplayStatement, session int, firstepoch float64,
    starttime time.Time, config Configuration) {
    fmt.Printf("[session %d] NEW SESSION\n", session)

    db, err := sql.Open("mysql", config.Dsn)
    if err != nil {
        panic(err.Error())
    }
    dbopen := true
    defer db.Close()

    last_stmt_epoch := firstepoch
    for {
        pkt := <-c
        if last_stmt_epoch != 0.0 {
            firsttime := timefromfloat(firstepoch)
            pkttime := timefromfloat(pkt.epoch)
            delaytime_orig := pkttime.Sub(firsttime)
            mydelay := time.Since(starttime)
            delaytime_new := delaytime_orig - mydelay

            fmt.Printf("[session %d] Sleeptime: %s\n", session,
                delaytime_new)
            time.Sleep(delaytime_new)
        }
        last_stmt_epoch = pkt.epoch
        switch pkt.cmd {
        case 14: // Ping
            continue
        case 1: // Quit
            fmt.Printf("[session %d] COMMAND REPLAY: QUIT\n", session)
            dbopen = false
            db.Close()
        case 3: // Query
            if dbopen == false {
                fmt.Printf("[session %d] RECONNECT\n", session)
                db, err = sql.Open("mysql", config.Dsn)
                if err != nil {
                    panic(err.Error())
                }
                dbopen = true
            }
            fmt.Printf("[session %d] STATEMENT REPLAY: %s\n", session,
                   pkt.stmt)
            _, err := db.Exec(pkt.stmt)
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
}

func main() {
    conffile, _ := os.Open("go-mysql-replay.conf.json")
    confdec := json.NewDecoder(conffile)
    config := Configuration{}
    err := confdec.Decode(&config)
    if err != nil {
        fmt.Printf("Error reading configuration from "+
            "'./go-mysql-replay.conf.json': %s\n", err)
    }

    fileflag := flag.String("f", "./test.dat",
        "Path to datafile for replay")
    flag.Parse()

    datFile, err := os.Open(*fileflag)
    if err != nil {
        fmt.Println(err)
    }

    reader := csv.NewReader(datFile)
    reader.Comma = '\t'
    var lineNum uint32 = 0

    var firstepoch float64 = 0.0
    starttime := time.Now()
    sessions := make(map[int]chan ReplayStatement)

    for {
        line, err := reader.Read()
        if err == io.EOF {
            break
        }
        lineNum++
        fmt.Println("[main] Processing line", lineNum)
        if err != nil {
            fmt.Println("[main] Error reading line", lineNum, "with error", err)
            continue
        }

        pkt, err := parsefields(line)
            if err != nil {
            fmt.Println("[main] Error parsing line", lineNum, "with error", err)
            continue
        }

        if firstepoch == 0.0 {
            firstepoch = pkt.epoch
        }

        if sessions[pkt.session] != nil {
            sessions[pkt.session] <- pkt
        } else {
            sess := make(chan ReplayStatement, 10000)
            sessions[pkt.session] = sess
            go mysqlsession(sessions[pkt.session], pkt.session,
                firstepoch, starttime, config)
            sessions[pkt.session] <- pkt
        }
    }
    fmt.Println("[main] Waiting for children to finish")
    for _, ch := range sessions {
        for {
            if(len(ch) == 0) {
                break
            }
            time.Sleep(100 * time.Millisecond)
        }
    }
}
