package mysql.dao;

import mysql.entity.Position;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class PositionDAO {
    private JdbcTemplate template;
    private SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");


    public void setDataSource(DataSource dataSource){
        this.template = new JdbcTemplate(dataSource);
    }

    public List findAll(long id){
        final String FIND_ALL_SQL =
                "select ID, SIM, SENDTIME, LON, LAT, DIRECTION, VELOCITY from HIS_LOCATION_"
                        +sdf.format(new Date())+" WHERE ID > ?";
        return this.template.query(
                FIND_ALL_SQL,
                new Object[]{id},
                new RowMapper() {
                    public Object mapRow(ResultSet resultSet, int i) throws SQLException {
                        Position position = new Position();
                        position.setId(resultSet.getLong("ID"));
                        position.setSim(resultSet.getString("SIM"));
                        position.setCreateTime(resultSet.getString("SENDTIME"));
                        position.setLongitude(0);
                        position.setLatitude(0);
                        position.setLongitudeValue(resultSet.getDouble("LON"));
                        position.setLatitudeValue(resultSet.getDouble("LAT"));
                        position.setDirection(resultSet.getDouble("DIRECTION"));
                        position.setSpeed(resultSet.getDouble("VELOCITY"));
                        return position;
                    }
                });
    }
}
