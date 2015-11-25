package hadoop.entity;

public abstract class Candata {

    public String device;//for RowId(deviceId+canCode+timestamp)
    public String content;
    public String createTime;

    @Override
    public String toString() {
        return String.format(
                "<Position: %s, %s, %s>",
                device, content, createTime);
    }
}
