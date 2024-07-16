import java.sql.*;
import java.util.Arrays;

public class MovieDataBase {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";

    private static final String DATABASE_NAME = "Movies";
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "admin";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

//            getMovies(connection); System.out.println();
//            getBooks(connection); System.out.println();
//            getSoundTracks(connection); System.out.println();
//
//            getThreeLongestMovies(connection); System.out.println();
//            showMovieActors(connection, "Титаник"); System.out.println();
//
////            addMovie(connection, "Terminator 2: Judgment Day", 1991, "fantasy", 137, 102000000, 516950043, new String[]{"Arnold Schwarzenegger", "Linda Hamilton", "Edward Furlong", "Robert Patrick", "Earl Boen"});
////            removeMovie(connection, "Terminator 2: Judgment Day"); System.out.println();
////            updateMovie(connection, 1, "Terminator 2: Judgment Day", 1991, "fantasy", 137, 102000000, 516950043, new String[]{"Arnold Schwarzenegger", "Linda Hamilton", "Edward Furlong", "Robert Patrick", "Earl Boen"});
//
//            getMoviesByFees(connection, 1000000000); System.out.println();
//            getSoundTracksByDiapason(connection, 240, 320); System.out.println();
            getBooksByGenre(connection, "Научная фантастика"); System.out.println();
//
//            getSoundTracksByMovie(connection, "Звездные войны: Эпизод IV – Новая надежда"); System.out.println();
//            getMovieActorsByBook(connection, "Гарри Поттер и философский камень");

        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }



    //TODO 1 - Вывести все фильмы
    private static void getMovies(Connection connection) throws SQLException {
        String columnName0 = "id",
                columnName1 = "movie_title",
                columnName2 = "movie_released",
                columnName3 = "movie_genre",
                columnName4 = "movie_duration",
                columnName5 = "movie_budget",
                columnName6 = "movie_fees",
                columnName7 = "movie_actors";
        int param0 = -1, param4 = -1, param5 = -1, param6 = -1;
        String param1 = null, param2 = null, param3 = null, param7 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.\"Movies\";");
        System.out.println("Фильмы:");
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param3 = rs.getString(columnName3);
            param4 = rs.getInt(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getInt(columnName6);
            param7 = rs.getString(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    //TODO 2 - Вывести все книги
    private static void getBooks(Connection connection) throws SQLException {
        String columnName0 = "id",
                columnName1 = "book_author",
                columnName2 = "book_title",
                columnName3 = "book_released",
                columnName4 = "film_based_on_this_book",
                columnName5 = "movie_id",
                columnName6 = "book_genre";
        int param0 = -1, param3 = -1, param5 = -1;
        String param1 = null, param2 = null, param4 = null, param6 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.\"Books\";");
        System.out.println("Книги:");
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getInt(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getString(columnName6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param6 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    //TODO 3 - Вывести все саундтреки
    private static void getSoundTracks(Connection connection) throws SQLException {
        String columnName0 = "id",
                columnName1 = "soundtrack_executor",
                columnName2 = "soundtrack_name",
                columnName3 = "soundtrack_genre",
                columnName4 = "soundtrack_released",
                columnName5 = "soundtrack_duration",
                columnName6 = "movie_with_this_soundtrack",
                columnName7 = "movie_id";
        int param0 = -1, param4 = -1, param5 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param6 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM public.\"Soundtracks\";");
        System.out.println("Саундтреки:");
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getInt(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    //TODO 4 - Вывести 3 самых долгих фильма
    private static void getThreeLongestMovies(Connection connection) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.\"Movies\" ORDER BY movie_duration DESC LIMIT 3");
        ResultSet rs = preparedStatement.executeQuery();
        System.out.println("Три самых долгих фильма:");
        String columnName0 = "id",
                columnName1 = "movie_title",
                columnName2 = "movie_released",
                columnName3 = "movie_genre",
                columnName4 = "movie_duration",
                columnName5 = "movie_budget",
                columnName6 = "movie_fees",
                columnName7 = "movie_actors";
        int param0 = -1, param4 = -1, param5 = -1, param6 = -1;
        String param1 = null, param2 = null, param3 = null, param7 = null;
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getInt(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getInt(columnName6);
            param7 = rs.getString(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    //TODO 5 - Показать актеров фильма
    private static void showMovieActors(Connection connection, String movie_title) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT movie_actors FROM public.\"Movies\"u WHERE movie_title = ?");
        preparedStatement.setString(1, movie_title);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            Array movieActorsArray = rs.getArray("movie_actors");
            if (movieActorsArray != null) {
                String[] movieActors = (String[]) movieActorsArray.getArray();
                String actorsResult = String.join(", ", movieActors);
                System.out.println("Актеры в фильме \"" + movie_title + "\": " + actorsResult);
            } else {
                System.out.println("Указанный фильм не найден");
            }
        }
    }

    //TODO 6 - Добавить фильм
    private static void addMovie(Connection connection, String movie_title, Integer movie_released, String movie_genre, Integer movie_duration, Integer movie_budget, Integer movie_fees, String[] movie_actors) throws SQLException {
        if (movie_title == null || movie_title.isBlank() || movie_released < 1900 || movie_released > 2025 || movie_genre == null || movie_genre.isBlank() || movie_duration <= 0 || movie_budget <= 0 || movie_fees <= 0 || movie_actors == null)
            return;
        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO public.\"Movies\"(movie_title, movie_released, movie_genre, movie_duration, movie_budget, movie_fees, movie_actors) VALUES (?, ?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, movie_title);    // "безопасное" добавление имени
        statement.setInt(2, movie_released);
        statement.setString(3, movie_genre);
        statement.setInt(4, movie_duration);
        statement.setInt(5, movie_budget);
        statement.setInt(6, movie_fees);
        statement.setArray(7, connection.createArrayOf("varchar", movie_actors));
        int count =
                statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор фильма " + rs.getInt(1));
        }
        System.out.println("Добавлен " + count + " фильм");
        getMovies(connection);
    }

    //TODO 7 - Удалить фильм
    private static void removeMovie(Connection connection, String movie_title) throws SQLException {
        if (movie_title == null || movie_title.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("DELETE from public.\"Movies\" WHERE movie_title=?;");
        statement.setString(1, movie_title);
        int count = statement.executeUpdate();
        if (count == 1) System.out.println("Удален 1 фильм");
        else System.out.println("Удалено " + count + " фильмов");
        getMovies(connection);
    }

    //TODO 8 - Изменить строчку с фильмом
    private static void updateMovie(Connection connection, Integer id, String movie_title, Integer movie_released, String movie_genre, Integer movie_duration, Integer movie_budget, Integer movie_fees, String[] movie_actors) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE public.\"Movies\" SET movie_title = ?, movie_released = ?, movie_genre = ?, movie_duration = ?, movie_budget = ?, movie_fees = ?, movie_actors = ? WHERE id = ?");
        preparedStatement.setString(1, movie_title);
        preparedStatement.setInt(2, movie_released);
        preparedStatement.setString(3, movie_genre);
        preparedStatement.setInt(4, movie_duration);
        preparedStatement.setInt(5, movie_budget);
        preparedStatement.setInt(6, movie_fees);
        preparedStatement.setArray(7, connection.createArrayOf("VARCHAR", movie_actors));
        preparedStatement.setInt(8, id);
        int count = preparedStatement.executeUpdate();
        if (count > 0) {
            System.out.println("Фильм с ID" + count + " изменен");
            getMovies(connection);
        } else {
            System.out.println("Фильм с указанным ID не найден");
        }
    }

    //TODO 9 - Найти все книги по жанру
    private static void getBooksByGenre(Connection connection, String book_genre) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.\"Books\" WHERE book_genre = ?");
        preparedStatement.setString(1, book_genre);
        ResultSet rs = preparedStatement.executeQuery();
        String columnName0 = "id",
                columnName1 = "book_author",
                columnName2 = "book_title",
                columnName3 = "book_released",
                columnName4 = "film_based_on_this_book",
                columnName5 = "movie_id",
                columnName6 = "book_genre";
        System.out.println("Книги жанра " + book_genre + ":");
        while (rs.next()) {
            int param0 = rs.getInt(columnName0);
            String param1 = rs.getString(columnName1);
            String param2 = rs.getString(columnName2);
            int param3 = rs.getInt(columnName3);
            String param4 = rs.getString(columnName4);
            int param5 = rs.getInt(columnName5);
            String param6 = rs.getString(columnName6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param6 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    //TODO 10  - найти все саундтреки длиной от и до
    private static void getSoundTracksByDiapason(Connection connection, int min_value_sec, int max_value_sec) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM public.\"Soundtracks\" WHERE soundtrack_duration > ? AND soundtrack_duration < ?");
        preparedStatement.setInt(1, min_value_sec);
        preparedStatement.setInt(2, max_value_sec);
        ResultSet rs = preparedStatement.executeQuery();
        String columnName0 = "id",
                columnName1 = "soundtrack_executor",
                columnName2 = "soundtrack_name",
                columnName3 = "soundtrack_genre",
                columnName4 = "soundtrack_released",
                columnName5 = "soundtrack_duration",
                columnName6 = "movie_with_this_soundtrack",
                columnName7 = "movie_id";
        int param0 = -1, param4 = -1, param5 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param6 = null;
        System.out.println("Саундтреки длительностью от " + min_value_sec + " до " + max_value_sec + " секунд:");
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getInt(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    //TODO 11 - Вывести все фильмы, сборы которых больше указанного числа
    private static void getMoviesByFees(Connection connection, int fees) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT movie_title, movie_released FROM public.\"Movies\" WHERE movie_fees > ?");
        preparedStatement.setInt(1, fees);
        ResultSet rs = preparedStatement.executeQuery();
        System.out.println("Фильмы, сборы которых больше " + fees + ":");
        while (rs.next()) {
            String movie_title = rs.getString("movie_title");
            int movie_released = rs.getInt("movie_released");
            System.out.println(movie_title + " - " + movie_released);
        }
    }

    //TODO 12 - Вывести все саундтреки, указав название фильма (JOIN)
    private static void getSoundTracksByMovie(Connection connection, String movie_title) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT * FROM public.\"Soundtracks\" s " +
                        "JOIN public.\"Movies\" m " +
                        "ON s.movie_id = m.id " +
                        "WHERE m.movie_title = ?");
        preparedStatement.setString(1, movie_title);
        ResultSet rs = preparedStatement.executeQuery();
        if (!rs.isBeforeFirst()) {
            System.out.println("Совпадений не найдено");
            return;
        }
        System.out.println("Саундтреки в фильме " + movie_title);
        String columnName0 = "id",
                columnName1 = "soundtrack_executor",
                columnName2 = "soundtrack_name",
                columnName3 = "soundtrack_genre",
                columnName4 = "soundtrack_released",
                columnName5 = "soundtrack_duration",
                columnName6 = "movie_with_this_soundtrack",
                columnName7 = "movie_id";
        int param0 = -1, param4 = -1, param5 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param6 = null;
        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getInt(columnName4);
            param5 = rs.getInt(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7);
        }
    }

    //TODO 13 - Вывести всех актеров фильма, указав название книги, по которой был сделан фильм (JOIN)
    private static void getMovieActorsByBook(Connection connection, String book_title) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT m.movie_actors, m.movie_title FROM public.\"Books\" b JOIN public.\"Movies\" m ON b.movie_id = m.id WHERE b.book_title = ?");
        preparedStatement.setString(1, book_title);
        ResultSet rs = preparedStatement.executeQuery();
        while (rs.next()) {
            Array actorsArray = rs.getArray("movie_actors");
            String movie_title = rs.getString("movie_title");
            String[] actors = (String[]) actorsArray.getArray();
            System.out.println("Актеры в фильме " + movie_title + ": " + Arrays.toString(actors));
        }
    }
}
