import mysql.connector
import csv

class VideoGameDatabase:
    
    def __init__(self, hostname, username, pw, database_name):
        self.db_connection = mysql.connector.connect(
            host=hostname,
            user=username,
            password=pw,
            database=database_name
        )

        self.cursor = self.db_connection.cursor(dictionary=True)

    @staticmethod
    def initialize_database(hostname, username, pw, database_name):
    
        mydb = mysql.connector.connect(
            host=hostname,
            user=username,
            password=pw
        )

        mycursor = mydb.cursor(dictionary=True)
        mycursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        mycursor.execute(f"USE {database_name};")
        mycursor.execute(f"SET foreign_key_checks=1;")

        VideoGameDatabase.create_tables(mycursor)
        VideoGameDatabase.populate_tables(mycursor)

        mydb.commit()
        mydb.close()
    
    @staticmethod
    def create_tables(mycursor):
        mycursor.execute(
            """
            CREATE TABLE genre
            (
                genre_id SMALLINT UNSIGNED AUTO_INCREMENT,
                genre_name VARCHAR(20),
                CONSTRAINT pk_genre PRIMARY KEY (genre_id, genre_name)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE publisher
            (
                publisher_id SMALLINT UNSIGNED AUTO_INCREMENT,
                publisher_name VARCHAR(50),
                CONSTRAINT pk_publisher PRIMARY KEY (publisher_id, publisher_name)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE platform
            (
                platform_id SMALLINT UNSIGNED AUTO_INCREMENT,
                platform_name VARCHAR(20),
                CONSTRAINT pk_platform PRIMARY KEY (platform_id , platform_name)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE game
            (
                game_id SMALLINT UNSIGNED AUTO_INCREMENT,
                game_name VARCHAR(200),
                platform_id SMALLINT UNSIGNED,
                publisher_id SMALLINT UNSIGNED,
                genre_id SMALLINT UNSIGNED,
                release_year YEAR,
                CONSTRAINT pk_game PRIMARY KEY (game_id, platform_id, publisher_id),
                CONSTRAINT fk_game_platform FOREIGN KEY (platform_id)
                    REFERENCES platform (platform_id),
                CONSTRAINT fk_game_publisher FOREIGN KEY (publisher_id)
                    REFERENCES publisher (publisher_id),
                CONSTRAINT fk_game_genre FOREIGN KEY (genre_id)
                    REFERENCES genre (genre_id)
            );
            """
        )

        mycursor.execute(
            """
            CREATE TABLE game_sales
            (
                sales_id SMALLINT UNSIGNED AUTO_INCREMENT,
                game_id SMALLINT UNSIGNED,
                na_sales FLOAT UNSIGNED,
                eu_sales FLOAT UNSIGNED,
                jp_sales FLOAT UNSIGNED,
                other_sales FLOAT UNSIGNED,
                global_sales FLOAT UNSIGNED,
                CONSTRAINT pk_sales PRIMARY KEY (sales_id, game_id),
                CONSTRAINT fk_game_sale_id FOREIGN KEY (game_id)
                    REFERENCES game (game_id)
            );
            """
        )


    @staticmethod
    def populate_tables(mycursor):
        
        publisher_names = set()
        genre_names = set()
        platform_names = set()

        with open('vgsales.csv', 'r') as csv_data:
            reader = csv.DictReader(csv_data)
            
            for item in reader:
                database_ids = {}
                if item["Publisher"] not in publisher_names:
                    mycursor.execute(
                        f"""
                        INSERT INTO publisher
                            (publisher_id, publisher_name)
                        VALUES
                            (null, "{item["Publisher"]}");
                        """
                    )
                    mycursor.execute("SELECT LAST_INSERT_ID() publisher_id")
                    database_ids.update(mycursor.fetchone())
                    publisher_names.add(item["Publisher"])
                else:
                    mycursor.execute(
                        f"""
                        SELECT publisher_id FROM publisher
                        WHERE publisher_name = "{item["Publisher"]}";
                        """
                    )
                    database_ids.update(mycursor.fetchone())

                if item["Genre"] not in genre_names:
                    mycursor.execute(
                        f"""
                        INSERT INTO genre
                            (genre_id, genre_name)
                        VALUES
                            (null, "{item["Genre"]}");
                        """
                    )
                    mycursor.execute("SELECT LAST_INSERT_ID() genre_id")
                    database_ids.update(mycursor.fetchone())
                    genre_names.add(item["Publisher"])
                else:
                    mycursor.execute(
                        f"""
                        SELECT genre_id FROM genre
                        WHERE genre_name = "{item["Genre"]}";
                        """
                    )
                    database_ids.update(mycursor.fetchone())

                if item["Platform"] not in platform_names:
                    mycursor.execute(
                        f"""
                        INSERT INTO platform
                            (platform_id, platform_name)
                        VALUES
                            (null, "{item["Platform"]}");
                        """
                    )
                    mycursor.execute("SELECT LAST_INSERT_ID() platform_id")
                    database_ids.update(mycursor.fetchone())
                    genre_names.add(item["Platform"])
                else:
                    mycursor.execute(
                        f"""
                        SELECT platform_id FROM platform
                        WHERE platform_name = "{item["Platform"]}";
                        """
                    )
                    database_ids.update(mycursor.fetchone())
                
                mycursor.execute(
                    f"""
                    INSERT INTO game
                        (game_id, game_name, platform_id, publisher_id, genre_id, release_year)
                    VALUES
                        (null, "{item["Name"]}", "{database_ids["platform_id"]}", "{database_ids["publisher_id"]}",
                        "{database_ids["genre_id"]}", {int(item["Year"]) if item["Year"] != "N/A" else "null"});
                    """
                )
                mycursor.execute("SELECT LAST_INSERT_ID() game_id")
                database_ids.update(mycursor.fetchone())
                
                mycursor.execute(
                    f"""
                    INSERT INTO game_sales
                        (sales_id, game_id, na_sales, eu_sales, jp_sales, other_sales, global_sales)
                    VALUES
                        (null, {database_ids["game_id"]}, {float(item["NA_Sales"]) if item["NA_Sales"] != "N/A" else "null"},
                        {float(item["EU_Sales"]) if item["EU_Sales"] != "N/A" else "null"},
                        {float(item["JP_Sales"]) if item["JP_Sales"] != "N/A" else "null"},
                        {float(item["Other_Sales"]) if item["Other_Sales"] != "N/A" else "null"},
                        {float(item["Global_Sales"]) if item["Global_Sales"] != "N/A" else "null"});
                    """
                )


    def top_k_sales(self, k):
        self.cursor.execute(
        f"""
        SELECT g.game_name, s.global_sales
        FROM game_sales s
            INNER JOIN game g
            ON s.game_id = g.game_id
        ORDER BY s.global_sales DESC
        LIMIT {k};
        """)
        for item in self.cursor.fetchall():
            print(item)
    

    def count_games_in_genre(self, genre_name=None):

        if not genre_name:
            self.cursor.execute(
            f"""
            SELECT ge.genre_name, COUNT(*) count
            FROM game_sales s
                INNER JOIN game g
                ON s.game_id = g.game_id
                INNER JOIN genre ge
                on g.genre_id = ge.genre_id
            GROUP BY ge.genre_name
            ORDER BY count DESC;
            """)
        else:
            self.cursor.execute(
            f"""
            SELECT ge.genre_name, COUNT(*) count
            FROM game_sales s
                INNER JOIN game g
                ON s.game_id = g.game_id
                INNER JOIN genre ge
                on g.genre_id = ge.genre_id
            WHERE ge.genre_name = "{genre_name}"
            GROUP BY ge.genre_name
            ORDER BY count DESC;
            """)

        for item in self.cursor.fetchall():
            print(item)


    def total_global_sales_per_platform(self, platform=None, year=None):

        if not platform:
            self.cursor.execute(
                f"""
                SELECT p.platform_name, ROUND(SUM(s.global_sales), 2) all_global_sales
                FROM game_sales s
                    INNER JOIN game g
                    ON s.game_id = g.game_id
                    INNER JOIN platform p
                    on p.platform_id = g.platform_id
                {"WHERE g.release_year IS NOT NULL" if not year else "WHERE g.release_year =  " + str(year)}
                GROUP BY p.platform_name
                ORDER BY p.platform_name;
                """)
        else:
            self.cursor.execute(
                f"""
                SELECT p.platform_name, ROUND(SUM(s.global_sales), 2) all_global_sales
                FROM game_sales s
                    INNER JOIN game g
                    ON s.game_id = g.game_id
                    INNER JOIN platform p
                    on p.platform_id = g.platform_id
                {"WHERE g.release_year IS NOT NULL" if not year else "WHERE g.release_year = " + str(year)} AND p.platform_name = "{platform}"
                GROUP BY p.platform_name
                ORDER BY p.platform_name;
                """)

        for item in self.cursor.fetchall():
            print(item)


    def number_of_games_per_year_by_publisher(self, publisher):
        self.cursor.execute(
            f"""
            SELECT g.release_year, COUNT(*) game_releases
            FROM game_sales s
                INNER JOIN game g
                ON s.game_id = g.game_id
                INNER JOIN publisher p
                on p.publisher_id = g.publisher_id
            WHERE g.release_year IS NOT NULL and p.publisher_name = "{publisher}"
            GROUP BY g.release_year;
            """)

        for item in self.cursor.fetchall():
            print(item)

    def close(self):
        self.cursor.close()
        self.db_connection.close()
    

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    
    VideoGameDatabase.initialize_database(os.getenv("DBHOST"), os.getenv("DBUSERNAME"), os.getenv("DBPASSWORD"), os.getenv("DATABASE"))
    vg_database = VideoGameDatabase(os.getenv("DBHOST"), os.getenv("DBUSERNAME"), os.getenv("DBPASSWORD"), os.getenv("DATABASE"))

    vg_database.top_k_sales(10)
    vg_database.count_games_in_genre()
    vg_database.count_games_in_genre("Racing")
    vg_database.total_global_sales_per_platform()
    vg_database.total_global_sales_per_platform(year=2016)
    vg_database.total_global_sales_per_platform("NES")
    vg_database.total_global_sales_per_platform("N64", 1998)
    vg_database.number_of_games_per_year_by_publisher("Nintendo")
    vg_database.number_of_games_per_year_by_publisher("THQ")

    vg_database.close()
