package top.dataframe.writer


import groovy.transform.CompileStatic
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import top.dataframe.DataFrame
import top.dataframe.val.RedisInfoKey

@CompileStatic
class RedisWriter implements DataFrameWriter,Serializable {
    private Map<String, Object> connectInfo
    String prefix
    String key
    String value

    RedisWriter(Map<String, Object> connectInfo, String prefix) {
        this.connectInfo = connectInfo
        this.prefix = prefix
        this.key = "key"
        this.value = "value"
    }

    RedisWriter(Map<String, Object> connectInfo, String prefix, String key) {
        this.connectInfo = connectInfo
        this.prefix = prefix
        this.key = key
    }

    RedisWriter(Map<String, Object> connectInfo, String prefix, String key, String value) {
        this.connectInfo = connectInfo
        this.prefix = prefix
        this.key = key
        this.value = value
    }

    @Override
    int write(DataFrame df) {
        Jedis jedis
        try {
            jedis = new Jedis(connectInfo.get(RedisInfoKey.HOST) as String, connectInfo.get(RedisInfoKey.PORT) as int)
            def auth = connectInfo.get(RedisInfoKey.PASSWORD)
            if (auth) {
                jedis.auth(auth as String)
            }
            def db = connectInfo.get(RedisInfoKey.DB)
            if (db) {
                jedis.select(db as int)
            }

            Pipeline pip = jedis.pipelined()
            df.rows.each { Map<String, Object> map ->
                if (value) {
                    pip.set(prefix + map.get(key), map.get(value) as String)
                } else {
                    // TODO
                    return 0
                }
            }

            pip.sync()

            return df.size()
        } finally {
            jedis?.close()
        }

        df.size()
    }

}
