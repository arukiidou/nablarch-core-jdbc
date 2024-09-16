package nablarch.core.db.dialect;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * MySQL用の方言を吸収するためのクラスです。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class MySQLDialect extends DefaultDialect {

    /** 一意制約違反(ER_DUP_ENTRY, SQLSTATE: 23000)を表すエラーコード */
    private static final int DUP_ENTRY_ERROR_CODE = 1062;

    /** Query Timeout(ER_QUERY_TIMEOUT, SQLSTATE: HY000)時に発生する例外のエラーコード */
    private static final int QUERY_TIMEOUT_ERROR_CODE = 3024;

    /** 検索結果の値変換クラス */
    // TODO 未実装
    private static final MySQLResultSetConvertor RESULT_SET_CONVERTOR = new MySQLResultSetConvertor();

    /**
     * コンストラクタ。
     *
     */
    public MySQLDialect() {

    }

    /**
     * {@inheritDoc}
     * <p/>
     * MySQLの場合、以下例外の場合タイムアウト対象の例外として扱う。
     * <ul>
     * <li>エラーコード:3024(クエリタイムアウト時に送出される例外)</li>
     * </ul>
     *
     * @param sqlException SQL例外
     * @return errorCode が {@link #QUERY_TIMEOUT_ERROR_CODE}の場合true.
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        final int errorCode = sqlException.getErrorCode();
        return errorCode == QUERY_TIMEOUT_ERROR_CODE;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * @return false固定
     */
    @Override
    public boolean supportsOffset() {
        return true;
    }

    /**
     * シーケンスはサポートしない。
     * <p/>
     * @return false固定
     */
    @Override
    public boolean supportsSequence() {
        return false;
    }

    // TODO 未実装
    @Override
    public ResultSetConvertor getResultSetConvertor() {
        return RESULT_SET_CONVERTOR;
    }

    /**
     * SQL例外が一意制約違反による例外かどうか判定する。
     * MySQLの場合、以下例外の場合意制約違反対象の例外として扱う。
     * <ul>
     * <li>エラーコード:1062(一意制約違反(ER_DUP_ENTRY, SQLSTATE: 23000))</li>
     * </ul>
     *
     * @param sqlException SQL例外
     * @return errorCode が {@link #DUP_ENTRY_ERROR_CODE}の場合true.
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        final int errorCode = sqlException.getErrorCode();
        return errorCode == DUP_ENTRY_ERROR_CODE;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * ページングの条件を元に、取得レコードをフィルタリングするSQLに変換する。
     * <p/>
     */
    // TODO 未実装
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        boolean hasOffset = selectOption.getOffset() > 0;

        StringBuilder result = new StringBuilder(256);
        result.append("SELECT SUB2.* FROM (SELECT SUB1.*, ROWNUM ROWNUM_ FROM (")
                .append(sql)
                .append(") SUB1 ) SUB2 WHERE");
        if (hasOffset) {
            result.append(" SUB2.ROWNUM_ > ")
                    .append(selectOption.getOffset());
        }

        if (selectOption.getLimit() > 0) {
            if (hasOffset) {
                result.append(" AND SUB2.ROWNUM_ <= ")
                        .append(selectOption.getOffset() + selectOption.getLimit());
            } else {
                result.append(" SUB2.ROWNUM_ <= ")
                        .append(selectOption.getLimit());
            }
        }
        return result.toString();
    }

    /**
     * ResultSetから値を取得するクラス。
     */
    // TODO 未実装
    private static class MySQLResultSetConvertor implements ResultSetConvertor {

        @Override
        public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            switch (rsmd.getColumnType(columnIndex)) {
                case Types.TIMESTAMP:
                    return rs.getTimestamp(columnIndex);
                case Types.DATE:
                    return rs.getTimestamp(columnIndex);
                default:
                    return rs.getObject(columnIndex);
            }
        }

        @Override
        public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return true;
        }
    }

    @Override
    public String getPingSql() {
        return "select 1";
    }
}
