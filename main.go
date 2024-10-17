package main

import (
	"database/sql"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	_ "github.com/proullon/ramsql/driver"
)

var port = "2567"

type Movie struct {
	ID          int64   `json:"id"`
	ImdbID      string  `json:"imdbID"`
	Title       string  `json:"title"`
	Year        int     `json:"year"`
	Rating      float64 `json:"rating"`
	IsSuperHero bool    `json:"isSuperHero"`
}

type movieLog struct {
	LogData string  `json:"logData"`
	ImdbID  string  `json:"imdbID"`
	Title   string  `json:"title"`
	Year    int     `json:"year"`
	Rating  float64 `json:"rating"`
	// IsSuperHero bool    `json:"isSuperHero"`
}

var movieLogs []movieLog

func getAllMoviesHandler(c echo.Context) error {
	mvs := []Movie{}
	y := c.QueryParam("year")

	if y == "" {
		rows, err := db.Query(`SELECT id, imdbID, title, year, rating, isSuperHero
		FROM goimdb`)
		if err != nil {
			log.Fatal("query error", err)
		}
		defer rows.Close()

		for rows.Next() {
			var m Movie
			if err := rows.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year, &m.Rating, &m.IsSuperHero); err != nil {
				return c.JSON(http.StatusInternalServerError, "scan:"+err.Error())
			}
			mvs = append(mvs, m)
		}

		if err := rows.Err(); err != nil {
			return c.JSON(http.StatusInternalServerError, err.Error())
		}

		return c.JSON(http.StatusOK, mvs)
	}

	year, err := strconv.Atoi(y)
	if err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	rows, err := db.Query(`SELECT id, imdbID, title, year, rating, isSuperHero
	FROM goimdb
	WHERE year = ?`, year)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var m Movie
		if err := rows.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year, &m.Rating, &m.IsSuperHero); err != nil {
			return c.JSON(http.StatusInternalServerError, err.Error())
		}
		mvs = append(mvs, m)
	}

	if err := rows.Err(); err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, mvs)
}

func getMoviesByIdHandler(c echo.Context) error {
	imdbID := c.Param("imdbID")

	row := db.QueryRow(`SELECT id, imdbID, title, year, rating, isSuperHero 
	FROM goimdb WHERE imdbID=?`, imdbID)
	m := Movie{}
	err := row.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year, &m.Rating, &m.IsSuperHero)
	switch err {
	case nil:
		return c.JSON(http.StatusOK, m)
	case sql.ErrNoRows:
		return c.JSON(http.StatusNotFound, map[string]string{"message!": "not found"})
	default:
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
}

func createMoviesHandler(c echo.Context) error {
	m := &Movie{}

	if err := c.Bind(m); err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	stmt, err := db.Prepare(`
	INSERT INTO goimdb(imdbID,title,year,rating,isSuperHero)
	VALUES (?,?,?,?,?);
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	defer stmt.Close()

	// b := fmt.Sprintf("%v", m.IsSuperHero)
	_, err = stmt.Exec(m.ImdbID, m.Title, m.Year, m.Rating, m.IsSuperHero)
	switch {
	case err == nil:
		// id, _ := r.LastInsertId()
		// m.ID = id

		logItem := movieLog{}
		logItem.LogData = "INSERT " + time.Now().String()
		logItem.ImdbID = m.ImdbID
		logItem.Title = m.Title
		logItem.Year = m.Year
		logItem.Rating = m.Rating
		// logItem.IsSuperHero = m.IsSuperHero
		movieLogs = append(movieLogs, logItem)
		return c.JSON(http.StatusCreated, movieLogs)
	case err.Error() == "UNIQUE constraint violation":
		return c.JSON(http.StatusConflict, "movie already exists")
	default:
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
}

func updateMoviesHandler(c echo.Context) error {
	m := &Movie{}

	if err := c.Bind(m); err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	// stmt, err := db.Prepare(`
	// UPDATE goimdb
	// SET year=?
	// WHERE imdbID=?
	// `)
	stmt, err := db.Prepare(`
	UPDATE goimdb
	SET title=$1
	,year=$2
	,rating=$3
	WHERE imdbID=$4
	`)

	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	defer stmt.Close()

	// _, err = stmt.Exec(m.Rating, m.ImdbID)
	_, err = stmt.Exec(m.Title, m.Year, m.Rating, m.ImdbID)
	switch {
	case err == nil:

		logItem := movieLog{}
		logItem.LogData = "UPDATE " + time.Now().String()
		logItem.ImdbID = m.ImdbID
		logItem.Title = m.Title
		logItem.Year = m.Year
		logItem.Rating = m.Rating
		// logItem.IsSuperHero = m.IsSuperHero
		movieLogs = append(movieLogs, logItem)
		return c.JSON(http.StatusCreated, movieLogs)
	case err.Error() == "UNIQUE constraint violation":
		return c.JSON(http.StatusConflict, "movie already exists")
	default:
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
}

var db *sql.DB

func conn() {
	var err error
	db, err = sql.Open("ramsql", "goimdb")
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	conn()

	createTb := `
	CREATE TABLE IF NOT EXISTS goimdb (
	id INT AUTO_INCREMENT,
	imdbID TEXT NOT NULL UNIQUE,
	title TEXT NOT NULL,
	year INT NOT NULL,
	rating FLOAT NOT NULL,
	isSuperHero BOOLEAN NOT NULL,
	PRIMARY KEY (id)
	);
	`
	if _, err := db.Exec(createTb); err != nil {
		log.Fatal("create table error", err)
	}

	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/movies", getAllMoviesHandler)
	e.GET("/movies/:imdbID", getMoviesByIdHandler)

	e.POST("/movies", createMoviesHandler)

	e.PUT("/movies", updateMoviesHandler)

	// port := "2565"
	log.Println("starting... port:", port)

	log.Fatal(e.Start(":" + port))
}
