package hadoop.entity;

public abstract class Position {

    public String device;//for RowId(deviceId+timestamp)
    public int longitude;
    public int latitude;
    public double longitudeValue;
    public double latitudeValue;
    public double speed;
    public double direction;
    public String createTime;

    @Override
    public String toString() {
        return String.format(
                "<Position: %s, %d, %d, %f, %f, %f, %f, %s>",
                device, longitude, latitude, longitudeValue,
                latitudeValue, speed, direction, createTime);
    }
}
