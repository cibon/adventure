package mysql.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by erik_mac on 11/27/15.
 */
public class DeviceDao {
    private JdbcTemplate template;
    public void setDataSource(DataSource dataSource){
        this.template = new JdbcTemplate(dataSource);
    }
    public String findDID(String deviceID){
        final String FIND_DID_SQL = "SELECT communication_code FROM devices WHERE device_id = ?";
        return (String)template.queryForObject(FIND_DID_SQL,
                new Object[]{deviceID},
                new RowMapper() {
                    public Object mapRow(ResultSet resultSet, int i) throws SQLException {
                        return resultSet.getString("communication_code");
                    }
                });
    }
}
