import connect

movies_df = connect.connect_postgres("movies")
users_df = connect.connect_postgres("users")
movies_df.show()
users_df.show()