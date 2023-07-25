package main

import (
	"database/sql"
	"datavisualization/model"
	"datavisualization/server"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	host              = "timescaledb"
	port              = 5432 // Default PostgreSQL port
	user              = "timescaledb"
	password          = "password"
	dbname            = "mydata"
	timeLayout        = "2006-01-02 15:04:05"
	batchSize         = 50 // Number of records to insert in each batch
	table_normal_us   = "normal_us"
	table_normal_ms   = "normal_ms"
	table_normal_50ms = "normal_50ms"
	table_normal_1s   = "normal_1s"
	filesNormalDir    = "data/normal/"
)

func main() {
	// Initialize logger with pretty console output
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout}).With().Caller().Logger()

	// Construct the connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// Open a connection to the database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to the database")
	}
	defer db.Close()

	// Check the connection
	err = db.Ping()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to ping the database")
	}

	log.Info().Msg("Connected to the database!")

	// Check if the table already exists
	// exists, err := checkTableExists(db, table_normal_us)
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to check if the table exists")
	// }

	// if !exists {
	// 	err = createTable(db, table_normal_us)
	// 	if err != nil {
	// 		log.Fatal().Err(err).Msg("Failed to create the table normal_us")
	// 	}
	// 	log.Info().Msg("Table normal_us created successfully!")
	// }

	// exists, err = checkTableExists(db, table_normal_ms)
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to check if the table exists")
	// }

	// if !exists {
	// 	err = createTableAggregate(db, table_normal_ms)
	// 	if err != nil {
	// 		log.Fatal().Err(err).Msg("Failed to create the table normal_ms")
	// 	}
	// 	log.Info().Msg("Table normal_ms created successfully!")
	// }

	// exists, err = checkTableExists(db, table_normal_50ms)
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to check if the table exists")
	// }

	// if !exists {
	// 	err = createTableAggregate(db, table_normal_50ms)
	// 	if err != nil {
	// 		log.Fatal().Err(err).Msg("Failed to create the table normal_50ms")
	// 	}
	// 	log.Info().Msg("Table normal_50ms created successfully!")
	// }

	// exists, err = checkTableExists(db, table_normal_1s)
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to check if the table exists")
	// }

	// if !exists {
	// 	err = createTableAggregate(db, table_normal_1s)
	// 	if err != nil {
	// 		log.Fatal().Err(err).Msg("Failed to create the table normal_1s")
	// 	}
	// 	log.Info().Msg("Table normal_1s created successfully!")
	// }

	// // // Get a list of files in the directory
	// fileList, err := getFileList(filesNormalDir)
	// if err != nil {
	// 	log.Debug().Err(err).Msg("Failed to get file list from directory")
	// }

	// // Read and insert data from each file
	// timenow := time.Now()
	// for _, file := range fileList {
	// 	filePath := filepath.Join(filesNormalDir, file)
	// 	log.Info().Msgf("read & insert file %v", filePath)
	// 	timenow, err = readAndInsertDataFromFile(db, table_normal_us, filePath, timenow)
	// 	if err != nil {
	// 		log.Error().Err(err).Msgf("Error processing file: %s", file)
	// 	} else {
	// 		log.Info().Msgf("Data from file '%s' inserted successfully!", file)
	// 	}
	// }

	server.Serve(db)
}

func calculateAndInsert(db *sql.DB, rows []model.RowAg, tableName string) error {
	// Aggregate values
	var (
		tachometer        float64
		uba_axial         float64
		uba_radial        float64
		uba_tangential    float64
		oba_axial         float64
		oba_radial        float64
		oba_tangential    float64
		microphone        float64
		tachometerMax     float64
		uba_axialMax      float64
		uba_radialMax     float64
		uba_tangentialMax float64
		oba_axialMax      float64
		oba_radialMax     float64
		oba_tangentialMax float64
		microphoneMax     float64
		tachometerMin     float64
		uba_axialMin      float64
		uba_radialMin     float64
		uba_tangentialMin float64
		oba_axialMin      float64
		oba_radialMin     float64
		oba_tangentialMin float64
		microphoneMin     float64
		rowsCount         int
	)

	for _, row := range rows {
		tachometer += row.TachometerAvg
		uba_axial += row.UbaAxialAvg
		uba_radial += row.UbaRadialAvg
		uba_tangential += row.UbaTangentialAvg
		oba_axial += row.ObaAxialAvg
		oba_radial += row.ObaRadialAvg
		oba_tangential += row.ObaTangentialAvg
		microphone += row.MicrophoneAvg

		if rowsCount == 0 {
			tachometerMax = row.TachometerAvg
			tachometerMin = row.TachometerAvg
			uba_axialMax = row.UbaAxialAvg
			uba_axialMin = row.UbaAxialAvg
			uba_radialMax = row.UbaRadialAvg
			uba_radialMin = row.UbaRadialAvg
			uba_tangentialMax = row.UbaTangentialAvg
			uba_tangentialMin = row.UbaTangentialAvg
			oba_axialMax = row.ObaAxialAvg
			oba_axialMin = row.ObaAxialAvg
			oba_radialMax = row.ObaRadialAvg
			oba_radialMin = row.ObaRadialAvg
			oba_tangentialMax = row.ObaRadialAvg
			oba_tangentialMin = row.ObaRadialAvg
			microphoneMax = row.MicrophoneAvg
			microphoneMin = row.MicrophoneAvg
		} else {
			if row.TachometerAvg > tachometerMax {
				tachometerMax = row.TachometerAvg
			}

			if row.TachometerAvg < tachometerMin {
				tachometerMin = row.TachometerAvg
			}

			if row.UbaAxialAvg > uba_axialMax {
				uba_axialMax = row.UbaAxialAvg
			}

			if row.UbaAxialAvg < uba_axialMin {
				uba_axialMin = row.UbaAxialAvg
			}

			if row.UbaRadialAvg > uba_radialMax {
				uba_radialMax = row.UbaRadialAvg
			}

			if row.UbaRadialAvg < uba_radialMin {
				uba_radialMin = row.UbaRadialAvg
			}

			if row.UbaTangentialAvg > uba_tangentialMax {
				uba_tangentialMax = row.UbaTangentialAvg
			}

			if row.UbaTangentialAvg < uba_tangentialMin {
				uba_tangentialMin = row.UbaTangentialAvg
			}

			if row.ObaAxialAvg > oba_axialMax {
				oba_axialMax = row.ObaAxialAvg
			}

			if row.ObaAxialAvg < oba_axialMin {
				oba_axialMin = row.ObaAxialAvg
			}

			if row.ObaRadialAvg > oba_radialMax {
				oba_radialMax = row.ObaRadialAvg
			}

			if row.ObaRadialAvg < oba_radialMin {
				oba_radialMin = row.ObaRadialAvg
			}

			if row.ObaTangentialAvg > oba_tangentialMax {
				oba_tangentialMax = row.ObaTangentialAvg
			}

			if row.ObaTangentialAvg < oba_tangentialMin {
				oba_tangentialMin = row.ObaTangentialAvg
			}

			if row.MicrophoneAvg > microphoneMax {
				microphoneMax = row.MicrophoneAvg
			}
			if row.MicrophoneAvg < microphoneMin {
				microphoneMin = row.MicrophoneAvg
			}
		}

		rowsCount++
	}

	tachometer /= float64(rowsCount)
	uba_axial /= float64(rowsCount)
	uba_radial /= float64(rowsCount)
	uba_tangential /= float64(rowsCount)
	oba_axial /= float64(rowsCount)
	oba_radial /= float64(rowsCount)
	oba_tangential /= float64(rowsCount)
	microphone /= float64(rowsCount)

	// Prepare the INSERT statement
	query := fmt.Sprintf(`
		INSERT INTO %s (
			event_time,
			tachometer,
			uba_axial,
			uba_radial,
			uba_tangential,
			oba_axial,
			oba_radial,
			oba_tangential,
			microphone,
			tachometerMax,
			uba_axialMax,
			uba_radialMax,
			uba_tangentialMax,
			oba_axialMax,
			oba_radialMax,
			oba_tangentialMax,
			microphoneMax,
			tachometerMin,
			uba_axialMin,
			uba_radialMin,
			uba_tangentialMin,
			oba_axialMin,
			oba_radialMin,
			oba_tangentialMin,
			microphoneMin
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
	`, tableName)

	_, err := db.Exec(
		query,
		pq.FormatTimestamp(rows[0].TimeEvent),
		tachometer,
		uba_axial,
		uba_radial,
		uba_tangential,
		oba_axial,
		oba_radial,
		oba_tangential,
		microphone,
		tachometerMax,
		uba_axialMax,
		uba_radialMax,
		uba_tangentialMax,
		oba_axialMax,
		oba_radialMax,
		oba_tangentialMax,
		microphoneMax,
		tachometerMin,
		uba_axialMin,
		uba_radialMin,
		uba_tangentialMin,
		oba_axialMin,
		oba_radialMin,
		oba_tangentialMin,
		microphoneMin,
	)
	if err != nil {
		return fmt.Errorf("failed to insert aggregated data: %w", err)
	}

	log.Info().Msgf("ta: %v, %v", tachometer, microphone)

	return nil
}

// Function to check if a table exists in the database
func checkTableExists(db *sql.DB, tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM   information_schema.tables
			WHERE  table_name = $1
		);
	`

	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func createTable(db *sql.DB, tableName string) error {
	// Create the table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			event_time TIMESTAMPTZ(6),
			tachometer FLOAT,
			uba_axial FLOAT,
			uba_radial FLOAT,
			uba_tangential FLOAT,
			oba_axial FLOAT,
			oba_radial FLOAT,
			oba_tangential FLOAT,
			microphone FLOAT
		);
		CREATE INDEX IF NOT EXISTS idx_event_time ON %s (event_time);
	`, tableName, tableName)

	_, err := db.Exec(createTableQuery)
	return err
}

func createTableAggregate(db *sql.DB, tableName string) error {
	// Create the table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			event_time TIMESTAMPTZ(3),
			tachometer FLOAT,
			uba_axial FLOAT,
			uba_radial FLOAT,
			uba_tangential FLOAT,
			oba_axial FLOAT,
			oba_radial FLOAT,
			oba_tangential FLOAT,
			microphone FLOAT,
			tachometerMax FLOAT,
			uba_axialMax FLOAT,
			uba_radialMax FLOAT,
			uba_tangentialMax FLOAT,
			oba_axialMax FLOAT,
			oba_radialMax FLOAT,
			oba_tangentialMax FLOAT,
			microphoneMax FLOAT,
			tachometerMin FLOAT,
			uba_axialMin FLOAT,
			uba_radialMin FLOAT,
			uba_tangentialMin FLOAT,
			oba_axialMin FLOAT,
			oba_radialMin FLOAT,
			oba_tangentialMin FLOAT,
			microphoneMin FLOAT
		);
		CREATE INDEX IF NOT EXISTS idx_event_time ON %s (event_time);
	`, tableName, tableName)

	_, err := db.Exec(createTableQuery)
	return err
}

// Function to get the list of files in a directory
func getFileList(dir string) ([]string, error) {
	var fileList []string

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".csv" {
			fileList = append(fileList, file.Name())
		}
	}

	return fileList, nil
}

// Function to read and insert data from a file
func readAndInsertDataFromFile(db *sql.DB, table, filePath string, timeStart time.Time) (time.Time, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return timeStart, fmt.Errorf("error opening file '%s': %w", filePath, err)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return timeStart, fmt.Errorf("error reading file '%s': %w", filePath, err)
	}

	records := strings.Split(string(data), "\n")
	if len(records) > 0 && records[len(records)-1] == "" {
		// Remove empty last record if present
		records = records[:len(records)-1]
	}

	batchCount := 0
	var stmt *sql.Stmt
	var tx *sql.Tx

	var rows1ms []model.RowAg
	var rows50ms []model.RowAg
	var rows1s []model.RowAg
	for _, record := range records {
		if batchCount%50 == 0 {
			tx, err = db.Begin()
			if err != nil {
				return timeStart, fmt.Errorf("error starting transaction: %w", err)
			}

			// Prepare the SQL statement for batch insert
			stmt, err = tx.Prepare(pq.CopyIn(table, "event_time", "tachometer", "uba_axial", "uba_radial", "uba_tangential", "oba_axial", "oba_radial", "oba_tangential", "microphone"))
			if err != nil {
				tx.Rollback()
				return timeStart, fmt.Errorf("error preparing batch insert statement: %w", err)
			}
		}

		fields := strings.Split(record, ",")

		if len(fields) != 8 {
			log.Warn().Msgf("Invalid record format: %s", record)
			continue
		}

		var columnValues []interface{}
		columnValues = append(columnValues, timeStart)
		for _, val := range fields {
			floatVal, err := strconv.ParseFloat(val, 64)
			if err != nil {
				log.Warn().Err(err).Msg("Error parsing float value")
				columnValues = append(columnValues, 0.0)
			} else {
				columnValues = append(columnValues, floatVal)
			}
		}

		d := model.RowAg{
			TimeEvent:        timeStart,
			TachometerAvg:    columnValues[1].(float64),
			UbaAxialAvg:      columnValues[2].(float64),
			UbaRadialAvg:     columnValues[3].(float64),
			UbaTangentialAvg: columnValues[4].(float64),
			ObaAxialAvg:      columnValues[5].(float64),
			ObaRadialAvg:     columnValues[6].(float64),
			ObaTangentialAvg: columnValues[7].(float64),
			MicrophoneAvg:    columnValues[8].(float64),
		}

		rows1ms = append(rows1ms, d)
		rows50ms = append(rows50ms, d)
		rows1s = append(rows1s, d)

		// Set time variable to current date and time with an interval of 20 microseconds
		timeStart = timeStart.Add(20 * time.Microsecond)

		// log.Info().Msgf("Add: %v", columnValues)

		// Append the current record values to the batch insert statement
		_, err = stmt.Exec(columnValues...)
		if err != nil {
			tx.Rollback()
			log.Warn().Err(err).Msg("Error executing batch insert statement")
			continue
		}

		batchCount++

		// Execute the batch insert when reaching the batch size
		if batchCount > 0 && batchCount%50 == 0 {
			_, err = stmt.Exec()
			if err != nil {
				tx.Rollback()
				log.Warn().Err(err).Msg("Error flushing batch insert")
				return timeStart, err
			}

			// Close the batch insert statement
			err = stmt.Close()
			if err != nil {
				tx.Rollback()
				log.Warn().Err(err).Msg("Error closing batch insert statement")
				return timeStart, err
			}

			err = tx.Commit()
			if err != nil {
				log.Warn().Err(err).Msg("Error committing transaction")
				return timeStart, err
			}

			err = calculateAndInsert(db, rows1ms, table_normal_ms)
			if err != nil {
				log.Warn().Err(err).Msg("while calculatin 1ms data")
			}
			rows1ms = nil

			if batchCount%2500 == 0 {
				err = calculateAndInsert(db, rows50ms, table_normal_50ms)
				if err != nil {
					log.Warn().Err(err).Msg("while calculatin 1ms data")
				}
				rows50ms = nil
			}

			if batchCount%50000 == 0 {
				err = calculateAndInsert(db, rows1s, table_normal_1s)
				if err != nil {
					log.Warn().Err(err).Msg("while calculatin 1ms data")
				}
				rows1s = nil
			}
		}
	}

	// Flush any remaining batch insert statements
	// if batchCount > 0 {
	// 	_, err = stmt.Exec()
	// 	if err != nil {
	// 		tx.Rollback()
	// 		log.Warn().Err(err).Msg("Error flushing batch insert")
	// 		return timeStart, err
	// 	}

	// 	// Close the batch insert statement
	// 	err = stmt.Close()
	// 	if err != nil {
	// 		tx.Rollback()
	// 		log.Warn().Err(err).Msg("Error closing batch insert statement")
	// 		return timeStart, err
	// 	}

	// 	// Commit the transaction
	// 	err = tx.Commit()
	// 	if err != nil {
	// 		log.Warn().Err(err).Msg("Error committing transaction")
	// 		return timeStart, err
	// 	}
	// }

	return timeStart, nil
}
