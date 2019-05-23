package redis.clients.jedis;

import java.util.*;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Debugger;

public class JedisSlotBasedConnectionHandler extends JedisClusterConnectionHandler {

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig) {
    super(nodes, poolConfig);
  }

  public Jedis getConnection() {
    // In antirez's redis-rb-cluster implementation,
    // getRandomConnection always return valid connection (able to
    // ping-pong)
    // or exception if all connections are invalid

    List<JedisPool> pools = getShuffledNodesPool();

    for (JedisPool pool : pools) {
      Jedis jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        String result = jedis.ping();

        if (result.equalsIgnoreCase("pong")) return jedis;

        pool.returnBrokenResource(jedis);
      } catch (JedisConnectionException ex) {
        if (jedis != null) {
          pool.returnBrokenResource(jedis);
        }
      } catch (Throwable ex) {
        if (jedis != null) {
          Debugger.log("PossibleJedisLeak :" + ex.getMessage(), ex);
        }
      }
    }

    throw new JedisConnectionException("no reachable node in cluster");
  }

  @Override
  public Jedis getConnectionFromSlot(int slot) {
    JedisPool connectionPool = cache.getSlotPool(slot);
    if (connectionPool != null) {
      // It can't guaranteed to get valid connection because of node
      // assignment
      return connectionPool.getResource();
    } else {
      return getConnection();
    }
  }

  private List<JedisPool> getShuffledNodesPool() {
    List<JedisPool> pools = new ArrayList<JedisPool>();
    pools.addAll(cache.getNodes().values());
    Collections.shuffle(pools);
    return pools;
  }

}
