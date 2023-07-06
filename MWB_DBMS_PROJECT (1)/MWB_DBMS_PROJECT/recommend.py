

from pyhive import hive
import pandas as pd

# Establish a connection to Hive
try:
    conn = hive.Connection(host="localhost", port=10000)
except Exception as e:
    print(f"Error connecting to Hive: {e}")
    exit(1)

# Function to execute a query and return the result as a DataFrame
def execute_query(query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

# Function to search for movies by title
def search_movies(search_string):
    query = f"SELECT * FROM movies WHERE title LIKE '%{search_string}%'"
    return execute_query(query)

# Function to fetch movie details by movie_id
def get_movie(movie_id):
    query = f"SELECT * FROM movies WHERE movie_id = {movie_id}"
    return execute_query(query)

# Function to get the top-rated movies in a specific genre
def get_top_rated_movies(genre, limit=10):
    query = f"""
        SELECT m.movie_id, m.title, AVG(r.rating) as avg_rating, COUNT(r.rating) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE m.genres LIKE '%%{genre}%%'
        GROUP BY m.movie_id, m.title
        HAVING num_ratings >= 10
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT {limit}
    """
    return execute_query(query)

def get_most_popular_movies(limit=10):
    query = f"""
        SELECT m.movie_id, m.title, COUNT(r.rating) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        GROUP BY m.movie_id, m.title
        ORDER BY num_ratings DESC
        LIMIT {limit}
    """
    return execute_query(query)

def get_movies_by_year(year, limit=10):
    query = f"""
        SELECT m.movie_id, m.title, COUNT(r.rating) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        WHERE YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(m.release_date, 'dd-MMM-yyyy')))) = {year}
        GROUP BY m.movie_id, m.title
        ORDER BY num_ratings DESC
        LIMIT {limit}
    """
    return execute_query(query)



def get_movies_by_genre(genre, limit=10):
    query = f"""
        SELECT m.movie_id, m.title
        FROM movies m
        WHERE m.genres LIKE '%%{genre}%%'
        LIMIT {limit}
    """
    return execute_query(query)

def get_similar_movies(movie_id, limit=5):
    query = f"""
        WITH movie_genres AS (
            SELECT genres
            FROM movies
            WHERE movie_id = {movie_id}
        ),
        similar_movies AS (
            SELECT m.movie_id, m.title, COUNT(r.rating) as num_ratings
            FROM ratings r
            JOIN movies m ON r.movie_id = m.movie_id
            WHERE m.genres IN (SELECT genres FROM movie_genres)
            AND m.movie_id != {movie_id}
            GROUP BY m.movie_id, m.title
            DISTRIBUTE BY num_ratings
            SORT BY num_ratings DESC
        )
        SELECT *
        FROM similar_movies
        LIMIT {limit}
    """
    return execute_query(query)


def get_top_rated_movies_by_demographics(demographic, value, limit=10):
    query = f"""
        SELECT m.movie_id, m.title, AVG(r.rating) as avg_rating, COUNT(r.rating) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        JOIN users u ON r.user_id = u.user_id
        WHERE u.{demographic} = '{value}'
        GROUP BY m.movie_id, m.title
        HAVING num_ratings >= 10
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT {limit}
    """
    return execute_query(query)

def top_rated_movies_by_decade(decade, limit):
    query = f"""
    SELECT m.movie_id, m.title, AVG(r.rating) as average_rating
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE YEAR(from_unixtime(unix_timestamp(m.release_date, 'dd-MMM-yyyy'))) >= {decade} AND YEAR(from_unixtime(unix_timestamp(m.release_date, 'dd-MMM-yyyy'))) < {decade + 10}
    GROUP BY m.movie_id, m.title
    ORDER BY average_rating DESC
    LIMIT {limit}
    """
    df = execute_query(query)
    return df



def get_genre_preferences_by_demographics(demographic, value, limit=5):
    query = f"""
        SELECT m.genres, COUNT(r.rating) as num_ratings
        FROM ratings r
        JOIN movies m ON r.movie_id = m.movie_id
        JOIN users u ON r.user_id = u.user_id
        WHERE u.{demographic} = '{value}'
        GROUP BY m.genres
        ORDER BY num_ratings DESC
        LIMIT {limit}
    """
    return execute_query(query)






def main_menu():
    while True:
        print("\nMovie Recommendation System:")
        print("1. Most popular movies")
        print("2. Movies from a specific year")
        print("3. Top-rated movies in a specific genre")
        print("4. Search movies by title")
        print("5. Get movie details by movie ID")
        print("6. Movies by genre")
        print("7. Top-rated movies by user demographics")
        print("8. Top-rated movies by decade")
        print("9. Genre preferences by demographic")
        print("10. Similar movies")
        print("11. Quit")
        choice = int(input("Enter your choice: "))

        if choice == 1:
            limit = int(input("Enter the number of movies to show: "))
            print(get_most_popular_movies(limit))
        elif choice == 2:
            year = int(input("Enter the year: "))
            limit = int(input("Enter the number of movies to show: "))
            print(get_movies_by_year(year, limit))
        elif choice == 3:
            genre = input("Enter the genre: ")
            limit = int(input("Enter the number of movies to show: "))
            print(get_top_rated_movies(genre, limit))
        elif choice == 4:
            search_string = input("Enter a search string: ")
            print(search_movies(search_string))
        elif choice == 5:
            movie_id = int(input("Enter the movie ID: "))
            print(get_movie(movie_id))
        elif choice == 6:
            genre = input("Enter the genre: ")
            limit = int(input("Enter the number of movies to show: "))
            print(get_movies_by_genre(genre, limit))
        elif choice == 7:
            demographic = input("Enter the demographic (age, gender, or occupation): ")
            value = input("Enter the demographic value: ")
            limit = int(input("Enter the number of movies to show: "))
            print(get_top_rated_movies_by_demographics(demographic, value, limit))
        elif choice == 8:
            decade = int(input("Enter the starting year of the decade (e.g., 1980 for 1980s): "))
            limit = int(input("Enter the number of movies to show: "))
            print(top_rated_movies_by_decade(decade, limit))
        elif choice == 9:
            demographic = input("Enter the demographic (age, gender, or occupation): ")
            value = input("Enter the demographic value: ")
            limit = int(input("Enter the number of genres to show: "))
            print(get_genre_preferences_by_demographics(demographic, value, limit))
        elif choice == 10:
            movie_id = int(input("Enter the movie ID: "))
            limit = int(input("Enter the number of similar movies to show: "))
            print(get_similar_movies(movie_id, limit))
        elif choice == 11:
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main_menu()







