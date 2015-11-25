package hadoop.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CandataDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("cans");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");

    public static final byte[] CONTENT_COL = Bytes.toBytes("content");
    public static final byte[] CREATE_TIME_COL = Bytes.toBytes("create_time");


    private static final Logger log = Logger.getLogger(CandataDAO.class);

    private Connection connection;

    public CandataDAO(Connection connection) {
        this.connection = connection;
    }

    private static Get mkGet(String candata) throws IOException {
        log.debug(String.format("Creating Get for %s", candata));

        Get g = new Get(Bytes.toBytes(candata));
        g.addFamily(INFO_FAM);
        return g;
    }

    private static Put mkPut(Candata candata) {
        log.debug(String.format("Creating Put for %s", candata));

        Put p = new Put(Bytes.toBytes(candata.device));
        p.addColumn(INFO_FAM, CONTENT_COL, Bytes.toBytes(candata.content));
        p.addColumn(INFO_FAM, CREATE_TIME_COL, Bytes.toBytes(candata.createTime));
        return p;
    }

    private static Scan mkScan() {
        Scan s = new Scan();
        s.addFamily(INFO_FAM);
        return s;
    }

    public void addCandata(String device,
                            String content,
                            String createTime)
            throws IOException {

        Table candatas = connection.getTable(TableName.valueOf(TABLE_NAME));

        Put p = mkPut(new Candata(device, content, createTime));
        candatas.put(p);

        candatas.close();
    }

    public void addCandatas(List<Candata> candataList)
            throws IOException {

        Table candatas = connection.getTable(TableName.valueOf(TABLE_NAME));
        List<Put> puts = new ArrayList<Put>();
        for(Candata candata: candataList){
            Put p = mkPut(candata);
            puts.add(p);
        }
        candatas.put(puts);
        candatas.close();
    }

    public Candata getCandata(String candata)
            throws IOException {
        Table candatas = connection.getTable(TableName.valueOf(TABLE_NAME));


        Get g = mkGet(candata);
        Result result = candatas.get(g);
        if (result.isEmpty()) {
            log.info(String.format("candata %s not found.", candata));
            return null;
        }

        Candata p = new Candata(result);
        candatas.close();
        return p;
    }


    public List<Candata> getCandatas()
            throws IOException {

        Table candatas = connection.getTable(TableName.valueOf(TABLE_NAME));


        ResultScanner results = candatas.getScanner(mkScan());
        ArrayList<Candata> ret
                = new ArrayList<Candata>();
        for (Result r : results) {
            ret.add(new Candata(r));
        }

        candatas.close();
        return ret;
    }

    public List<Candata> getCandatas(String deviceId, String code, String startTime, String stopTime)
            throws IOException {

        Table candatas = connection.getTable(TableName.valueOf(TABLE_NAME));

        Scan s = mkScan();
        s.setStartRow(Bytes.toBytes(deviceId+code+startTime));
        s.setStopRow(Bytes.toBytes(deviceId+code+stopTime));
        ResultScanner results = candatas.getScanner(s);
        ArrayList<Candata> ret
                = new ArrayList<Candata>();
        for (Result r : results) {
            ret.add(new Candata(r));
        }

        candatas.close();
        return ret;
    }


    public static class Candata
            extends hadoop.entity.Candata {
        private Candata(Result r) {
            this(r.getRow() == null
                            ? Bytes.toBytes(0L)
                            : r.getRow(),
                    r.getValue(INFO_FAM, CONTENT_COL),
                r.getValue(INFO_FAM, CREATE_TIME_COL)
            );
        }

        private Candata(byte[] device,
                        byte[] content,
                        byte[] createTime) {
            this(Bytes.toString(device),
                    Bytes.toString(content),
                    Bytes.toString(createTime)
            );

        }

        public Candata(String device,
                         String content,
                         String createTime) {
            this.device = device;
            this.content = content;
            this.createTime = createTime;
        }


    }
}
