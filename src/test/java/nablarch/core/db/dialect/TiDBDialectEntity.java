package nablarch.core.db.dialect;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * Dialectテスト用のテーブル
 */
@Entity
@Table(name = "TIDBDIALECT")
public class TiDBDialectEntity {

    @Id
    @Column(name = "entity_id", length = 18, nullable = false)
    public Long id;

    @Column(name = "str", length = 10)
    public String string;

    @Column(name = "num", length = 9)
    public Integer numeric;

    @Column(name = "big_int", length = 10)
    public Long bigInt;

    @Column(name = "date_col", columnDefinition = "date")
    @Temporal(TemporalType.DATE)
    public Date date;

    @Column(name = "timestamp_col", columnDefinition = "timestamp")
    public Timestamp timestamp;

    @Column(name = "decimal_col", precision = 15, scale = 5)
    public BigDecimal decimal;

    @Column(name = "binary_col")
    public byte[] binary;

    @Column(name = "vchar_col", columnDefinition = "VARCHAR(7)")
    public String varchar;

    @Column(name = "char_col", columnDefinition = "CHAR(7)")
    public String char7;

    @Column(name = "datetime_col", columnDefinition = "DATETIME(3)")
    public LocalDateTime dateTime;

    public TiDBDialectEntity() {
    }

    public TiDBDialectEntity(Long id, String str) {
        this.id = id;
        this.string = str;
    }

    public TiDBDialectEntity(
            Long id, String string, Integer numeric, Long bigInt, Date date, BigDecimal decimal,
            Timestamp timestamp, byte[] binary,
            String varchar, String char7,
            LocalDateTime dateTime) {
        this.id = id;
        this.string = string;
        this.numeric = numeric;
        this.bigInt = bigInt;
        this.date = date;
        this.decimal = decimal;
        this.timestamp = timestamp;
        this.binary = binary;
        this.varchar = varchar;
        this.char7 = char7;
        this.dateTime = dateTime;
    }
}
