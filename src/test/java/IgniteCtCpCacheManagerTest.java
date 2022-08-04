import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.configuration.WALMode;
import org.junit.jupiter.api.Test;

import ignite.config.IgniteConfigFactory;
import ignite.data.CacheKey;
import ignite.data.ContractDTOSimple;

class IgniteCtCpCacheManagerTest {

	IgniteConfigFactory configFactory = new IgniteConfigFactory();

	/**
	 * @param withPersistence:       Enable/disable persistence
	 * @param pageReplacementMode
	 * @param writeThrottlingEnabled
	 * @param walSegmentSize
	 * @param walStorePath
	 * @param walArchivePath
	 * @param checkpointFreq
	 * @param walMode
	 * @param storagePath
	 * @param cacheMode
	 * @param atomicityMode
	 * @param dataClass:             the class to be wrapped in
	 *                               ignite.data.DataWrapper<T>
	 * @param entriesCount           how many entries in total to be inserted in the
	 *                               cache
	 * @param batchSize:             number of entries that will be inserted in one
	 *                               call to cache.putAll
	 * @param workdir
	 * @param <T>
	 */
	private <T> void runTest(boolean withPersistence, PageReplacementMode pageReplacementMode,
			boolean writeThrottlingEnabled, int walSegmentSize, String walStorePath, String walArchivePath,
			int checkpointFreq, WALMode walMode, String storagePath, CacheMode cacheMode,
			CacheAtomicityMode atomicityMode, Class<T> dataClass, long entriesCount, long  batchSize, String workDir) {

		// Ignite configuration
		IgniteConfiguration igniteConfiguration = configFactory.produceIgniteConfiguration(withPersistence,
				pageReplacementMode, writeThrottlingEnabled, walSegmentSize, walStorePath, walArchivePath,
				checkpointFreq, walMode, storagePath, workDir);

		// Create cache configuration
		CacheConfiguration cacheConfiguration = configFactory.produceCacheConfiguration(cacheMode, atomicityMode,
				"cache");

		// Create Ignite

		try (Ignite ignite = Ignition.start(igniteConfiguration)) {
			ignite.cluster().state(ClusterState.ACTIVE);

			IgniteCache<CacheKey, ContractDTOSimple> cache = ignite.getOrCreateCache(cacheConfiguration);

			cache.clear();

			// insert data
			final long[] dataGroupIds = { 5, 36, 96, 14, 12, 28 };
			Random random = new Random();

			Map<CacheKey, ContractDTOSimple> map;
			for (int i = 1; i <= entriesCount / batchSize; i++) {
				map = new HashMap<>();
				for (int j = 1; j < batchSize; j++) {
					long dataGroupId = dataGroupIds[random.nextInt(dataGroupIds.length)];
					String softkey = UUID.randomUUID().toString();
					CacheKey k = new CacheKey(dataGroupId, softkey);
					ContractDTOSimple dto = new ContractDTOSimple();
					dto.dataGroupId = dataGroupId;
					dto.softLinkKey = softkey;

					map.put(k, dto);
				}

				Instant startInsert = Instant.now();
				cache.putAll(map);
				System.out.printf("batch %d put took  %d ms %n", i,
						Duration.between(startInsert, Instant.now()).toMillis());
			}

			
			String sql = "select  distinct  dataGroupId from "
					+ ContractDTOSimple.class.getSimpleName().toLowerCase() + ";";
			System.out.println("Executing " + sql);

			
			SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
			List<Long> results = new ArrayList<>();
			System.out
					.println(cache.withKeepBinary().query(new SqlFieldsQuery("explain plan for " + sql)).getAll().get(0).get(0));
			Instant startQuery = Instant.now();
			cache.withKeepBinary().query(sqlFieldsQuery).forEach(row -> results.add((Long) row.get(0)));
			System.out.printf("Query %d ms %n", Duration.between(startQuery, Instant.now()).toMillis());
			System.out.println(Arrays.toString(results.toArray()));

			startQuery = Instant.now();
			long count = getCount(ContractDTOSimple.class.getSimpleName().toLowerCase(), cache);
			System.out.printf("Count %d ms %n", Duration.between(startQuery, Instant.now()).toMillis());
			System.out.println("Total count in cache: " + count);
		}

	}

	private <T> long getCount(String tableName, IgniteCache<CacheKey, ContractDTOSimple> cache) {
		return (long) cache.withKeepBinary()
				.query(new SqlFieldsQuery("select  count(*) from " + tableName + ";"))
				.getAll().get(0).get(0);
	}

	@Test
	void testWithPersistenceSimpleClass() {
		runTest(true, PageReplacementMode.CLOCK, false, 64 * 1024 * 1024, "db/wal", "db/wal/archive", 180000,
				WALMode.NONE, null, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, ContractDTOSimple.class, 1L * 655_360,
				65_536, Path.of("./target/ignite").toAbsolutePath().toString());
	}
}