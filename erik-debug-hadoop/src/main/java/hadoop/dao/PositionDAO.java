package hadoop.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PositionDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("positions");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");

    public static final byte[] LONGITUDE_COL = Bytes.toBytes("longitude");
    public static final byte[] LATITUDE_COL = Bytes.toBytes("latitude");
    public static final byte[] LONGITUDE_VALUE_COL = Bytes.toBytes("longitude_value");
    public static final byte[] LATITUDE_VALUE_COL = Bytes.toBytes("latitude_value");
    public static final byte[] SPEED_COL = Bytes.toBytes("speed");
    public static final byte[] DIRECTION_COL = Bytes.toBytes("direction");
    public static final byte[] CREATE_TIME_COL = Bytes.toBytes("create_time");


    private static final Logger log = Logger.getLogger(PositionDAO.class);

    private Connection connection;

    public PositionDAO(Connection connection) {
        this.connection = connection;
    }

    private static Get mkGet(String position) throws IOException {
        log.debug(String.format("Creating Get for %s", position));

        Get g = new Get(Bytes.toBytes(position));
        g.addFamily(INFO_FAM);
        return g;
    }

    private static Put mkPut(Position position) {
        log.debug(String.format("Creating Put for %s", position));

        Put p = new Put(Bytes.toBytes(position.device));
        p.addColumn(INFO_FAM, LONGITUDE_COL, Bytes.toBytes(position.longitude));
        p.addColumn(INFO_FAM, LATITUDE_COL, Bytes.toBytes(position.latitude));
        p.addColumn(INFO_FAM, LONGITUDE_VALUE_COL, Bytes.toBytes(position.longitudeValue));
        p.addColumn(INFO_FAM, LATITUDE_VALUE_COL, Bytes.toBytes(position.latitudeValue));
        p.addColumn(INFO_FAM, SPEED_COL, Bytes.toBytes(position.speed));
        p.addColumn(INFO_FAM, DIRECTION_COL, Bytes.toBytes(position.direction));
        p.addColumn(INFO_FAM, CREATE_TIME_COL, Bytes.toBytes(position.createTime));
        return p;
    }

    private static Scan mkScan() {
        Scan s = new Scan();
        s.addFamily(INFO_FAM);
        return s;
    }

    public void addPosition(String device,
                            int longitude,
                            int latitude,
                            double longitudeValue,
                            double latitudeValue,
                            double speed,
                            double direction,
                            String createTime)
            throws IOException {

        Table positions = connection.getTable(TableName.valueOf(TABLE_NAME));

        Put p = mkPut(new Position(device, longitude, latitude,
                longitudeValue, latitudeValue, speed, direction, createTime));
        positions.put(p);

        positions.close();
    }

    public void addPositions(List<Position> positionList)
            throws IOException {

        Table positions = connection.getTable(TableName.valueOf(TABLE_NAME));
        List<Put> puts = new ArrayList<Put>();
        for(Position position: positionList){
            Put p = mkPut(position);
            puts.add(p);
        }
        positions.put(puts);
        positions.close();
    }

    public Position getPosition(String position)
            throws IOException {
        Table positions = connection.getTable(TableName.valueOf(TABLE_NAME));


        Get g = mkGet(position);
        Result result = positions.get(g);
        if (result.isEmpty()) {
            log.info(String.format("position %s not found.", position));
            return null;
        }

        Position p = new Position(result);
        positions.close();
        return p;
    }


    public List<Position> getPositions()
            throws IOException {

        Table positions = connection.getTable(TableName.valueOf(TABLE_NAME));


        ResultScanner results = positions.getScanner(mkScan());
        ArrayList<Position> ret
                = new ArrayList<Position>();
        for (Result r : results) {
            ret.add(new Position(r));
        }

        positions.close();
        return ret;
    }

    public List<Position> getPositions(String deviceId, String startTime, String stopTime)
            throws IOException {

        Table positions = connection.getTable(TableName.valueOf(TABLE_NAME));

        Scan s = mkScan();
        s.setStartRow(Bytes.toBytes(deviceId+startTime));
        s.setStopRow(Bytes.toBytes(deviceId+stopTime));
        ResultScanner results = positions.getScanner(s);
        ArrayList<Position> ret
                = new ArrayList<Position>();
        for (Result r : results) {
            ret.add(new Position(r));
        }

        positions.close();
        return ret;
    }


    public static class Position
            extends hadoop.entity.Position {
        private Position(Result r) {
            this(r.getRow() == null
                            ? Bytes.toBytes(0L)
                            : r.getRow(),
                r.getValue(INFO_FAM, LONGITUDE_COL) == null
                        ? Bytes.toBytes(0)
                        : r.getValue(INFO_FAM, LONGITUDE_COL),
                r.getValue(INFO_FAM, LATITUDE_COL) == null
                        ? Bytes.toBytes(0)
                        : r.getValue(INFO_FAM, LATITUDE_COL),
                r.getValue(INFO_FAM, LONGITUDE_VALUE_COL) == null
                        ? Bytes.toBytes(0.0)
                        : r.getValue(INFO_FAM, LONGITUDE_VALUE_COL),
                r.getValue(INFO_FAM, LATITUDE_VALUE_COL) == null
                        ? Bytes.toBytes(0.0)
                        : r.getValue(INFO_FAM, LATITUDE_VALUE_COL),
                r.getValue(INFO_FAM, SPEED_COL) == null
                        ? Bytes.toBytes(0.0)
                        : r.getValue(INFO_FAM, SPEED_COL),
                r.getValue(INFO_FAM, DIRECTION_COL) == null
                        ? Bytes.toBytes(0.0)
                        : r.getValue(INFO_FAM, DIRECTION_COL),
                r.getValue(INFO_FAM, CREATE_TIME_COL)
            );
        }

        private Position(byte[] device,
                         byte[] longitude,
                         byte[] latitude,
                         byte[] longitudeValue,
                         byte[] latitudeValue,
                         byte[] speed,
                         byte[] direction,
                         byte[] createTime) {
            this(Bytes.toString(device),
                    Bytes.toInt(longitude),
                    Bytes.toInt(latitude),
                    Bytes.toDouble(longitudeValue),
                    Bytes.toDouble(latitudeValue),
                    Bytes.toDouble(speed),
                    Bytes.toDouble(direction),
                    Bytes.toString(createTime)
            );

        }

        public Position(String device,
                         int longitude,
                         int latitude,
                         double longitudeValue,
                         double latitudeValue,
                         double speed,
                         double direction,
                         String createTime) {
            this.device = device;
            this.longitude = longitude;
            this.latitude = latitude;
            this.longitudeValue = longitudeValue;
            this.latitudeValue = latitudeValue;
            this.speed = speed;
            this.direction = direction;
            this.createTime = createTime;
        }


    }
}
