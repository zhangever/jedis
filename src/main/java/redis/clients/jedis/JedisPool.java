package redis.clients.jedis;

import java.net.URI;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Debugger;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.JedisURIHelper;
import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> {

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host) {
    this(poolConfig, host, Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(String host, int port) {
    this(new GenericObjectPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, null,
        Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final String host) {
    URI uri = URI.create(host);
    if (uri.getScheme() != null && uri.getScheme().equals("redis")) {
      String h = uri.getHost();
      int port = uri.getPort();
      String password = JedisURIHelper.getPassword(uri);
      int database = 0;
      Integer dbIndex = JedisURIHelper.getDBIndex(uri);
      if (dbIndex != null) {
        database = dbIndex.intValue();
      }
      this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(h, port,
          Protocol.DEFAULT_TIMEOUT, password, database, null), new GenericObjectPoolConfig());
    } else {
      this.internalPool = new GenericObjectPool<Jedis>(new JedisFactory(host,
          Protocol.DEFAULT_PORT, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null),
          new GenericObjectPoolConfig());
    }
  }

  public JedisPool(final URI uri) {
    this(new GenericObjectPoolConfig(), uri, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final URI uri, final int timeout) {
    this(new GenericObjectPoolConfig(), uri, timeout);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password) {
    this(poolConfig, host, port, timeout, password, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port) {
    this(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, final int port,
      final int timeout) {
    this(poolConfig, host, port, timeout, null, Protocol.DEFAULT_DATABASE, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database) {
    this(poolConfig, host, port, timeout, password, database, null);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password, final int database, final String clientName) {
    super(poolConfig, new JedisFactory(host, port, timeout, password, database, clientName));
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri) {
    this(poolConfig, uri, Protocol.DEFAULT_TIMEOUT);
  }

  public JedisPool(final GenericObjectPoolConfig poolConfig, final URI uri, final int timeout) {
    super(poolConfig, new JedisFactory(uri.getHost(), uri.getPort(), timeout,
        JedisURIHelper.getPassword(uri),
        JedisURIHelper.getDBIndex(uri) != null ? JedisURIHelper.getDBIndex(uri) : 0, null));
  }

  @Override
  public Jedis getResource() {
    Jedis jedis = super.getResource();

    //todo
    if (jedis != null) {
      Client client = jedis.getClient();
      Debugger.addConn(client.getHost() + ":" + client.getPort());
    }

    jedis.setDataSource(this);
    return jedis;
  }

  public void returnBrokenResource(final Jedis resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
      //todo
      Debugger.removeConn();
    }
  }

  public void returnResource(final Jedis resource) {
    if (resource != null) {
      try {
    	  try {
    		  resource.resetState();
    	  } catch (Exception e) {
    		  Debugger.log("resetState failed when returnResource", e);
    		  throw e;
    	  }
    	  try {
    		  returnResourceObject(resource);
    	  } catch (Exception e) {
    		  throw e;
    	  }
          //todo
          Debugger.removeConn();
      } catch (Exception e) {
        returnBrokenResource(resource);
        throw new JedisException("Could not return the resource to the pool", e);
      }
    }
  }
  
  public JedisPool() {
	  
  }
  
  public static void main(String[] args) {
	  Jedis connection = new Jedis("127.0.0.1", 6379);
	  new JedisPool().returnResource(connection);
  }

  public int getNumActive() {
    if (this.internalPool == null || this.internalPool.isClosed()) {
      return -1;
    }

    return this.internalPool.getNumActive();
  }
}
