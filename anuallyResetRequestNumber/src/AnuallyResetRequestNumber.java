import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AnuallyResetRequestNumber {

    public static void main(String[] args) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String url = "jdbc:oracle:thin:@localhost:1521:orcl";
            String user = "jtimar_mes";
            String password = "oracle";
            connection = DriverManager.getConnection(url, user, password);

            connection.setAutoCommit(false);

            String prefixQuery = "select PREFIX_ID from CMMS_DEFINITION_PREFIX where PREFIX_ID > 0";
            preparedStatement = connection.prepareStatement(prefixQuery);
            resultSet = preparedStatement.executeQuery();

            List<Long> prefixIds = new ArrayList<>();

            while (resultSet.next()) {
                prefixIds.add(resultSet.getLong(1));
            }


            for (Long prefixId : prefixIds) {
                String sequenceName = "seq_reqNO_" + prefixId;
                String query = "select substr(TO_CHAR(" + sequenceName + ".nextval), 1, 4) + 1||'00000' from dual";
                preparedStatement = connection.prepareStatement(query);

                int seq = 1;
                try {
                    resultSet = preparedStatement.executeQuery();
                    resultSet.next();
                    seq = resultSet.getInt(1);
                    String dropQuery = "DROP SEQUENCE " + sequenceName;
                    preparedStatement = connection.prepareStatement(dropQuery);
                    preparedStatement.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String increaseSeqQuery = "CREATE SEQUENCE " + sequenceName + " START WITH ? INCREMENT BY 1";
                String replace = increaseSeqQuery.replace("?", String.valueOf(seq));
                preparedStatement = connection.prepareStatement(replace);
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            commit(connection);
            close(resultSet, preparedStatement, connection);
        }
    }


    public static void close(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        try {
            if (resultSet != null)
                resultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (preparedStatement != null)
                preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void commit(Connection connection) {
        try {
            if (connection != null)
                connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
