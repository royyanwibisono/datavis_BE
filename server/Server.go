package server

import (
	"database/sql"
	"datavisualization/model"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/rs/zerolog/log"
)

type PostsResponse struct {
	Keys      []string
	DataValue [][]interface{}
	RowCount  int
}

func Serve(db *sql.DB) {
	app := fiber.New()

	// CORS middleware with AllowOrigins set to wildcard "*"
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
	}))

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, Fiber!")
	})

	app.Post("/timerange", func(c *fiber.Ctx) error {
		res := c.FormValue("res")
		//;
		query := fmt.Sprintf("SELECT MAX(event_time) AS max_value, MIN(event_time) AS min_value FROM normal_%s", res)
		log.Info().Msgf("q: %v", query)
		rows, err := db.Query(query)
		if err != nil {
			log.Error().Err(err).Msg("Error querying the database:")
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}
		defer rows.Close()

		var post model.TimeRange
		if rows.Next() {
			if err := rows.Scan(&post.TMax, &post.TMin); err != nil {
				log.Error().Err(err).Msg("Error reading data from the database:")
				return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
			}
		}

		return c.JSON(post)
	})

	app.Post("/chart", func(c *fiber.Ctx) error {
		res := c.FormValue("res")
		tstart := c.FormValue("ts")
		tend := c.FormValue("te")
		// Sample data

		query := fmt.Sprintf("SELECT * FROM normal_%s WHERE event_time >= '%s'::timestamp AND event_time <= '%s'::timestamp LIMIT 2000", res, tstart, tend)
		log.Info().Msgf("q: %v", query)
		rows, err := db.Query(query)
		if err != nil {
			log.Error().Err(err).Msg("Error querying the database:")
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}
		defer rows.Close()

		var postsResponse PostsResponse
		var keys []string
		var dataValue [][]interface{}

		if res == "us" {
			keys = append(keys, "TimeEvent", "Tachometer", "UbaAxial", "UbaRadial", "UbaTangential", "ObaAxial", "ObaRadial", "ObaTangential", "Microphone")
			for rows.Next() {
				var post model.Row
				if err := rows.Scan(&post.TimeEvent, &post.Tachometer, &post.UbaAxial, &post.UbaRadial, &post.UbaTangential, &post.ObaAxial, &post.ObaRadial, &post.ObaTangential, &post.Microphone); err != nil {
					log.Error().Err(err).Msg("Error reading data from the database:")
					return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
				}
				// Append the values to the dataValue slice as a single row
				dataValue = append(dataValue, []interface{}{post.TimeEvent, post.Tachometer, post.UbaAxial, post.UbaRadial, post.UbaTangential, post.ObaAxial, post.ObaRadial, post.ObaTangential, post.Microphone})
			}
		} else {
			keys = append(keys, "TimeEvent", "Tachometer", "UbaAxial", "UbaRadial", "UbaTangential", "ObaAxial", "ObaRadial", "ObaTangential", "Microphone",
				"TachometerMax", "UbaAxialMax", "UbaRadialMax", "UbaTangentialMax", "ObaAxialMax", "ObaRadialMax", "ObaTangentialMax", "MicrophoneMax",
				"TachometerMin", "UbaAxialMin", "UbaRadialMin", "UbaTangentialMin", "ObaAxialMin", "ObaRadialMin", "ObaTangentialMin", "MicrophoneMin")

			for rows.Next() {
				var post model.RowAg
				if err := rows.Scan(&post.TimeEvent, &post.TachometerAvg, &post.UbaAxialAvg, &post.UbaRadialAvg, &post.UbaTangentialAvg, &post.ObaAxialAvg, &post.ObaRadialAvg, &post.ObaTangentialAvg, &post.MicrophoneAvg,
					&post.TachometerMax, &post.UbaAxialMax, &post.UbaRadialMax, &post.UbaTangentialMax, &post.ObaAxialMax, &post.ObaRadialMax, &post.ObaTangentialMax, &post.MicrophoneMax,
					&post.TachometerMin, &post.UbaAxialMin, &post.UbaRadialMin, &post.UbaTangentialMin, &post.ObaAxialMin, &post.ObaRadialMin, &post.ObaTangentialMin, &post.MicrophoneMin); err != nil {
					log.Error().Err(err).Msg("Error reading data from the database:")
					return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
				}
				// Append the values to the dataValue slice as a single row
				dataValue = append(dataValue, []interface{}{post.TimeEvent, post.TachometerAvg, post.UbaAxialAvg, post.UbaRadialAvg, post.UbaTangentialAvg, post.ObaAxialAvg, post.ObaRadialAvg, post.ObaTangentialAvg, post.MicrophoneAvg,
					post.TachometerMax, post.UbaAxialMax, post.UbaRadialMax, post.UbaTangentialMax, post.ObaAxialMax, post.ObaRadialMax, post.ObaTangentialMax, post.MicrophoneMax,
					post.TachometerAvg, post.UbaAxialMin, post.UbaRadialMin, post.UbaTangentialMin, post.ObaAxialMin, post.ObaRadialMin, post.ObaTangentialMin, post.MicrophoneMin})
			}
		}

		// Populate the response struct
		postsResponse.Keys = keys
		postsResponse.DataValue = dataValue
		postsResponse.RowCount = len(dataValue)

		// Return the data as a JSON response
		return c.JSON(postsResponse)
	})

	log.Info().Msg("Server run at port 7000")

	app.Listen(":7000")
}
