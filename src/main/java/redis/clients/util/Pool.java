package redis.clients.util;

import java.io.Closeable;
import java.util.NoSuchElementException;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> implements Closeable {
  protected GenericObjectPool<T> internalPool;

  /**
   * Using this constructor means you have to set and initialize the internalPool yourself.
   */
  public Pool() {
  }

  @Override
  public void close() {
    closeInternalPool();
  }

  public boolean isClosed() {
    return this.internalPool.isClosed();
  }

  public Pool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
    initPool(poolConfig, factory);
  }

  public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {

    if (this.internalPool != null) {
      try {
        closeInternalPool();
      } catch (Exception e) {
      }
    }

    this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
  }

  public T getResource() {
    try {
      return internalPool.borrowObject();
    } catch (Exception e) {
      int numActive = internalPool.getNumActive();
      int maxTotal = internalPool.getMaxTotal();
      int idle = internalPool.getNumIdle();
      int waitings = internalPool.getNumWaiters();
      Debugger.log("current pool status:"
              + ", active:" + numActive
              + ", maxTotal:" + maxTotal
              + ", idle:" + idle
              + ", waitings:" + waitings);
      Debugger.log(e);

      if (e instanceof NoSuchElementException) {
        throw (NoSuchElementException)e;
      }
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  public void returnResourceObject(final T resource) {
    if (resource == null) {
      return;
    }
    try {
      internalPool.returnObject(resource);
      Debugger.removeConn((Jedis)resource);
    } catch (Exception e) {
      Debugger.log("returnResourceObject failed when returnResource " + e.getMessage(), e);
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  public void returnBrokenResource(final T resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  public void returnResource(final T resource) {
    if (resource != null) {
      returnResourceObject(resource);
    }
  }

  public void destroy() {
    closeInternalPool();
  }

  protected void returnBrokenResourceObject(final T resource) {
    try {
      internalPool.invalidateObject(resource);
      //todo
      Debugger.removeConn((Jedis) resource);
    } catch (Exception e) {
    	Debugger.log("invalidateObject failed when returnBrokenResourceObject " + e.getMessage(), e);
        throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  protected void closeInternalPool() {
    try {
      internalPool.close();
    } catch (Exception e) {
      throw new JedisException("Could not destroy the pool", e);
    }
  }
}
