package mysql.entity;


public class Position {
    private long id;
    private String sim;
    private int longitude;
    private int latitude;
    private double longitudeValue;
    private double latitudeValue;
    private double speed;
    private double direction;
    private String createTime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public String getSim() {
        return sim;
    }

    public void setSim(String sim) {
        this.sim = sim;
    }

    public int getLongitude() {
        return longitude;
    }

    public void setLongitude(int longitude) {
        this.longitude = longitude;
    }

    public int getLatitude() {
        return latitude;
    }

    public void setLatitude(int latitude) {
        this.latitude = latitude;
    }

    public double getLongitudeValue() {
        return longitudeValue;
    }

    public void setLongitudeValue(double longitudeValue) {
        this.longitudeValue = longitudeValue;
    }

    public double getLatitudeValue() {
        return latitudeValue;
    }

    public void setLatitudeValue(double latitudeValue) {
        this.latitudeValue = latitudeValue;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public double getDirection() {
        return direction;
    }

    public void setDirection(double direction) {
        this.direction = direction;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
