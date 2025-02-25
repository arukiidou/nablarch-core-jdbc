package nablarch.core.db.dialect;

import java.sql.SQLException;

import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;
import nablarch.core.db.dialect.DefaultDialect;

/**
 * TiDB用のSQL方言クラス。
 *
 * @author junya koyama
 */
@Published(tag = "architect")
public class TiDBDialect extends DefaultDialect {

    /** 一意制約違反を表すSQLState */
    private static final String UNIQUE_ERROR_SQL_STATE = "1062";

    /** Query Timeアウト時に発生する例外のエラーコード */
    private static final String QUERY_CANCEL_SQL_STATE = "3024";

    /**
     * {@inheritDoc}
     * <p/>
     * PostgreSQLでは、IDENTITYカラム(serial)を使用できるため、 {@code true}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * PostgreSQLでは、batch insertでIDENTITYカラムが使用できるため、{@code true}を返す。
     */
    @Override
    public boolean supportsIdentityWithBatchInsert() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * PostgreSQLでは、シーケンスオブジェクトが使用できるので、 {@code true}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * PostgreSQLでは、{@code offset}がサポートされるので{@code true}を返す。
     */
    @Override
    public boolean supportsOffset() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@link SQLException#getSQLState()}が23505(unique_violation:一意制約違反)の場合、一意制約違反とする。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return UNIQUE_ERROR_SQL_STATE.equals(sqlException.getSQLState());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * PostgreSQLの場合、以下例外の場合タイムアウト対象の例外として扱う。
     * <ul>
     * <li>SQLState:57014(query_canceled:クエリタイムアウト時に送出される例外)</li>
     * </ul>
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        final String sqlState = sqlException.getSQLState();
        return QUERY_CANCEL_SQL_STATE.equals(sqlState);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@code nextval}関数を使用して、次の順序を取得するSQL文を構築する。
     */
    @Override
    public String buildSequenceGeneratorSql(String sequenceName) {
        return "select nextval(" + sequenceName + ")";
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@code limit}と{@code offset}を使用したSQL文に変換する。
     * <ul>
     * <li> offsetのみ指定がある場合、limitはINTEGER_MAXとみなす(TiDBにおいて、Offsetだけ指定することができないため)
     * @see <a href="https://docs.pingcap.com/ja/tidb/stable/sql-statement-select">TiDB docs - 結果をページ分けする</a>
     */
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        final StringBuilder result = new StringBuilder(256);
        result.append(sql);

        // Example:
        // SELECT * FROM table_a t ORDER BY gmt_modified DESC LIMIT offset, row_count;

        // TiDBは引数が 1 つの場合、引数は返される行の最大数を指定します。
        // TiDBは引数が 2 つの場合、最初の引数は返される最初の行のオフセットを指定し、2 番目の引数は返される行の最大数を指定します。
        if (selectOption.getLimit() > 0) {
            result.append(" limit ");
            if (selectOption.getOffset() > 0) {
                result.append(selectOption.getOffset()).append(", ");
            }
            result.append(selectOption.getLimit());

        } else if (selectOption.getOffset() > 0) {
            result.append(" limit ").append(selectOption.getOffset())
                .append(", ").append(Integer.MAX_VALUE);
        }
        return result.toString();
    }

    @Override
    public String getPingSql() {
        return "select 1";
    }
}