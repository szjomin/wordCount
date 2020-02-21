package wordCount.common;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyProperties
{
  public Properties getProperties(String file) throws IOException
  {
    Properties props = null;
    InputStream in = null;

    try {
      in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream(file));
      props = new Properties();
      props.load(in);
      return props;
    } catch (IOException e) {
      throw e;
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
