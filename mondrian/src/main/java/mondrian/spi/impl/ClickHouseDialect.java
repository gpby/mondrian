package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Logger;

/**
 * This is a Clickhouse dialect for Mondrian.
 * <p>Being a subclass of {@link JdbcDialectImpl}, we also need to override
 * the {@link #FACTORY} static field.
 */
public class ClickHouseDialect extends JdbcDialectImpl {

    private static final Logger LOGGER =
            Logger.getLogger( ClickHouseDialect.class.getCanonicalName() );

    static {
        LOGGER.info( "ClickHouseDialect loaded in classloader." );
    }

    //
    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                    ClickHouseDialect.class,
                    DatabaseProduct.CLICKHOUSE);

    public ClickHouseDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public String toUpper(String expr) {
        return "upperUTF8(" + expr + ")";
    }

    @Override
    public boolean allowsCountDistinct() {
        return true;
    }

    @Override
    protected void quoteTimestampLiteral(
            StringBuilder buf,
            String value,
            Timestamp timestamp)
    {
        buf.append("toDateTime(");
        mondrian.olap.Util.singleQuoteString(value, buf);
        buf.append(")");
    }

    @Override
    protected void quoteDateLiteral(
            StringBuilder buf,
            String value,
            Date date)
    {
        // ClickHouse accepts toDate('2008-01-23') but not SQL:2003 format.
        buf.append("toDate(");
        mondrian.olap.Util.singleQuoteString(value, buf);
        buf.append(")");
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return true;
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }
}
