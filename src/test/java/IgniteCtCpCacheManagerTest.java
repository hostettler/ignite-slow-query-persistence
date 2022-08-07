import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
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

	enum QUERY_MODE {
		SQL_QUERY, PARTITION_SQL_QUERY, PARTITIONED_SCAN_QUERY
	}

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
	 * @throws InterruptedException
	 */
	private <T> void runTest(boolean withPersistence, PageReplacementMode pageReplacementMode,
			boolean writeThrottlingEnabled, int walSegmentSize, String walStorePath, String walArchivePath,
			int checkpointFreq, WALMode walMode, String storagePath, CacheMode cacheMode,
			CacheAtomicityMode atomicityMode, Class<T> dataClass, long entriesCount, long batchSize, String workDir,
			QUERY_MODE queryMode) throws InterruptedException {

		System.out.println("*************************************************************");
		System.out.println("*************************************************************");
		System.out.println(" Entries : " + entriesCount);
		System.out.println(" Persistence : " + withPersistence);
		System.out.println(" Throttling : " + writeThrottlingEnabled);
		System.out.println(" PageReplacement : " + pageReplacementMode);
		System.out.println(" WAL MODE : " + walMode);
		System.out.println(" QUERY_MODE : " + queryMode);
		System.out.println("*************************************************************");
		System.out.println("*************************************************************");

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

			Instant startInsert = Instant.now();
			insertData(entriesCount, batchSize, cache);
			System.out.printf("Inserting data took %d ms %n", Duration.between(startInsert, Instant.now()).toMillis());
//			
//			System.out.println("Wait 120s to let the grid cool down.");
//			Thread.sleep(120_000);
//
			ExecutorService executor = Executors.newFixedThreadPool(12);
			int partitionBatchSize = 64;
			int partitionsCount = ignite.affinity(cache.getName()).partitions();

			String sql = "select  distinct  dataGroupId from " + ContractDTOSimple.class.getSimpleName().toLowerCase()
					+ ";";

			switch (queryMode) {
			case SQL_QUERY:
				performSqlQuery(cache, sql);
				break;
			case PARTITION_SQL_QUERY:
				performSqlPerPartition(ignite, cache, sql, partitionsCount, executor, partitionBatchSize);
				break;
			case PARTITIONED_SCAN_QUERY:
				performScanQuery(ignite, cache, partitionsCount, executor, partitionBatchSize);
				break;

			}

			performSqlCount(cache);

			ignite.close();
		}

	}

	private void insertData(long entriesCount, long batchSize, IgniteCache<CacheKey, ContractDTOSimple> cache) {
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
	}

	private void performSqlPerPartition(Ignite ignite, IgniteCache<CacheKey, ContractDTOSimple> cache, String sql,
			int partitionBatchSize, ExecutorService executor, int partitionsCount) throws InterruptedException {
		Instant startQuery;
		System.out.println("*************************************************");
		System.out.println("******* Execute SQL Query per partition *********");
		System.out.println("*************************************************");

		SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
		Set<Long> myResults = new HashSet<Long>();
		List<Callable<Set<Long>>> callableTasks = new ArrayList<>();

		SqlFieldsQuery[] queries = new SqlFieldsQuery[partitionsCount / partitionBatchSize];

		for (int i = 0; i < partitionsCount / partitionBatchSize; i++) {
			SqlFieldsQuery q = new SqlFieldsQuery(sqlFieldsQuery);
			int[] parts = new int[partitionBatchSize];
			for (int j = 0; j < partitionBatchSize; j++)
				parts[j] = partitionBatchSize * i + j;
			q.setPartitions(parts);
			queries[i] = q;
		}

		for (int j = 0; j < partitionsCount / partitionBatchSize; j++) {
			final int index = j;
			Callable<Set<Long>> callableTask = () -> {
				Set<Long> set = new HashSet<Long>();
				cache.withKeepBinary().query(queries[index]).forEach(row -> {
					set.add((Long) row.get(0));
				});
				return set;
			};
			callableTasks.add(callableTask);
		}

		startQuery = Instant.now();
		List<Future<Set<Long>>> futures = executor.invokeAll(callableTasks);
		futures.forEach(t -> {
			try {
				myResults.addAll(t.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		});
		System.out.printf("Distinct SQL Partitioned Query %d ms %n",
				Duration.between(startQuery, Instant.now()).toMillis());
		System.out.println(myResults);

	}

	private SqlFieldsQuery performSqlQuery(IgniteCache<CacheKey, ContractDTOSimple> cache, String sql) {
		System.out.println("*************************************************");
		System.out.println("*************** Execute SQL Query ***************");
		System.out.println("*************************************************");

		System.out.println("Executing " + sql);

		SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
		List<Long> results = new ArrayList<>();
		System.out.println(
				cache.withKeepBinary().query(new SqlFieldsQuery("explain plan for " + sql)).getAll().get(0).get(0));
		Instant startQuery = Instant.now();
		cache.withKeepBinary().query(sqlFieldsQuery).forEach(row -> results.add((Long) row.get(0)));
		System.out.printf("Query %d ms %n", Duration.between(startQuery, Instant.now()).toMillis());
		System.out.println(Arrays.toString(results.toArray()));
		return sqlFieldsQuery;
	}

	private void performSqlCount(IgniteCache<CacheKey, ContractDTOSimple> cache) {
		Instant startQuery;
		System.out.println("*************************************************");
		System.out.println("*************** Execute SQL Count ***************");
		System.out.println("*************************************************");

		startQuery = Instant.now();
		long count = getCount(ContractDTOSimple.class.getSimpleName().toLowerCase(), cache);
		System.out.printf("Count %d ms %n", Duration.between(startQuery, Instant.now()).toMillis());
		System.out.println("Total count in cache: " + count);
	}

	private void performScanQuery(Ignite ignite, IgniteCache<CacheKey, ContractDTOSimple> cache, int partitionsCount,
			ExecutorService executor, int partitionBatchSize) throws InterruptedException {

		BinaryObjectBuilder builder = ignite.binary().builder(CacheKey.class.getCanonicalName());
		builder.setField("dataGroupId", 2L);
		builder.setField("softLinkKey", "");
		BinaryObject template = builder.build();
		BinaryField dataGroupId = template.type().field("dataGroupId");
		Set<Long> myResults = new HashSet<Long>();

		Instant startQuery;
		List<Callable<Set<Long>>> callableTasks;
		List<Future<Set<Long>>> futures;
		System.out.println("*************************************************");
		System.out.println("******* Execute Scan Query per partition ********");
		System.out.println("*************************************************");

		myResults.clear();
		callableTasks = new ArrayList<>();

		for (int i = 0; i < partitionsCount / partitionBatchSize; i++) {
			final int p = i;
			Callable<Set<Long>> callableTask = () -> {
				Set<Long> set = new HashSet<Long>();
				for (int j = 0; j < 64; j++) {
					ScanQuery<BinaryObject, BinaryObject> scanQuery = new ScanQuery<>();
					scanQuery.setPartition(p * partitionBatchSize + j);
					QueryCursor<Entry<BinaryObject, BinaryObject>> cursor = cache.withKeepBinary().query(scanQuery);
					cursor.forEach(tuple -> {
						set.add(dataGroupId.value(tuple.getKey()));
					});
				}
				return set;
			};
			callableTasks.add(callableTask);
		}

		startQuery = Instant.now();
		futures = executor.invokeAll(callableTasks);
		futures.forEach(t -> {
			try {
				myResults.addAll(t.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		});
		System.out.printf("Distinct Scan Query %d ms %n", Duration.between(startQuery, Instant.now()).toMillis());
		System.out.println(myResults);
	}

	private <T> long getCount(String tableName, IgniteCache<CacheKey, ContractDTOSimple> cache) {
		return (long) cache.withKeepBinary().query(new SqlFieldsQuery("select  count(*) from " + tableName + ";"))
				.getAll().get(0).get(0);
	}

	public static void main(String[] args) throws InterruptedException {
		new IgniteCtCpCacheManagerTest().testWithPersistenceSimpleClass();
	}

	@Test
	void testWithPersistenceSimpleClass() throws InterruptedException {
		String igniteWorkDirPath = "./target/ignite"; // "C:\\temp";

		Boolean persistence = Boolean.valueOf(System.getProperty("persistence", "NONE"));
		WALMode walMode = WALMode.valueOf(System.getProperty("walMode", "NONE"));
		Boolean throtting = Boolean.valueOf(System.getProperty("throtting", "false"));
		PageReplacementMode pageReplacementMode = PageReplacementMode
				.valueOf(System.getProperty("PageReplacementMode", "CLOCK"));
		QUERY_MODE queryMode = QUERY_MODE.valueOf(System.getProperty("queryMode", "PARTITIONED_SCAN_QUERY"));
		Long multiplicator =Long.valueOf(System.getProperty("multiplicator", "1"));

		runTest(persistence, pageReplacementMode, throtting, (2 * 1024 * 1024 * 1024) - 1, "db/wal", "db/wal/archive",
				180000, walMode, null, CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, ContractDTOSimple.class,
				multiplicator * 655_360L, 65_536, Path.of(igniteWorkDirPath).toAbsolutePath().toString(), queryMode);
	}
}
