import sqlite3
from flask import Flask, request, jsonify, g

app = Flask(__name__)
DATABASE = "movie_database.db"


# Database Connection and Teardown
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # For returning rows as dictionaries
    return g.db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "db"):
        g.db.close()


# Create Movie Database Tables
def create_tables():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        year_of_release INTEGER
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS actors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS technicians (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS movie_actor (
        movie_id INTEGER,
        actor_id INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies (id),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    );
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS movie_technician (
        movie_id INTEGER,
        technician_id INTEGER,
        FOREIGN KEY (movie_id) REFERENCES movies (id),
        FOREIGN KEY (technician_id) REFERENCES technicians (id)
    );
    """
    )

    conn.commit()


# Initialize the database tables
with app.app_context():
    create_tables()


# Define Error Handling
def handle_error(message, status_code):
    response = jsonify({"message": message})
    response.status_code = status_code
    return response


# API Endpoint for Movies (with Pagination)
@app.route("/movies", methods=["GET", "POST"])
def movies():
    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        offset = (page - 1) * per_page

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, name, year_of_release FROM movies LIMIT ? OFFSET ?;",
            (per_page, offset),
        )
        movies_list = [
            {
                "id": row["id"],
                "name": row["name"],
                "year_of_release": row["year_of_release"],
            }
            for row in cursor.fetchall()
        ]

        conn.close()
        return jsonify(movies_list)

    elif request.method == "POST":
        data = request.json
        if "name" not in data or "year_of_release" not in data:
            return handle_error(
                "Missing required fields (name and year_of_release)", 400
            )

        # Extract actors and technicians data
        actors = data.get("actors", [])
        technicians = data.get("technicians", [])

        conn = get_db()
        cursor = conn.cursor()

        # Insert movie information into the movies table
        cursor.execute(
            "INSERT INTO movies (name, year_of_release) VALUES (?, ?);",
            (data["name"], data["year_of_release"]),
        )
        conn.commit()

        movie_id = cursor.lastrowid  # Get the ID of the newly added movie

        # Insert actors and technicians into their respective tables and associate with the movie
        for actor_name in actors:
            cursor.execute("INSERT INTO actors (name) VALUES (?);", (actor_name,))
            actor_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO movie_actor (movie_id, actor_id) VALUES (?, ?);",
                (movie_id, actor_id),
            )

        for technician_name in technicians:
            cursor.execute(
                "INSERT INTO technicians (name) VALUES (?);", (technician_name,)
            )
            technician_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO movie_technician (movie_id, technician_id) VALUES (?, ?);",
                (movie_id, technician_id),
            )

        conn.commit()
        conn.close()

        return jsonify(
            {"message": "Movie and associated actors/technicians added successfully"},
            201,
        )


# API Endpoint for a Specific Movie
@app.route("/movies/<int:movie_id>", methods=["GET", "PUT"])
def movie(movie_id):
    if request.method == "GET":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, year_of_release FROM movies WHERE id = ?;", (movie_id,)
        )
        movie_data = cursor.fetchone()
        conn.close()

        if not movie_data:
            return handle_error("Movie not found", 404)

        return jsonify(
            {
                "id": movie_data["id"],
                "name": movie_data["name"],
                "year_of_release": movie_data["year_of_release"],
            }
        )

    elif request.method == "PUT":
        data = request.json
        if "name" not in data or "year_of_release" not in data:
            return handle_error(
                "Missing required fields (name and year_of_release)", 400
            )

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE movies SET name = ?, year_of_release = ? WHERE id = ?;",
            (data["name"], data["year_of_release"], movie_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"message": "Movie updated successfully"})


# API Endpoint to Delete an Actor (if not associated with movies)
@app.route("/actors/<int:actor_id>", methods=["DELETE"])
def delete_actor(actor_id):
    conn = get_db()
    cursor = conn.cursor()

    # Check if the actor is associated with movies
    cursor.execute("SELECT 1 FROM movie_actor WHERE actor_id = ? LIMIT 1;", (actor_id,))
    if cursor.fetchone() is not None:
        # The actor is associated with movies, so disassociate them
        cursor.execute("DELETE FROM movie_actor WHERE actor_id = ?;", (actor_id,))

    # Delete the actor from the actors table
    cursor.execute("DELETE FROM actors WHERE id = ?;", (actor_id,))

    conn.commit()
    conn.close()

    return jsonify({"message": "Actor deleted successfully"})


if __name__ == "__main__":
    app.run(debug=True)
