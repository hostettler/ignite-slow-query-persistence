package ignite.config;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneNoopCommunicationSpi;
import org.apache.ignite.spi.discovery.isolated.IsolatedDiscoverySpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;

import ignite.data.CacheKey;
import ignite.data.ContractDTOSimple;

public class IgniteConfigFactory
{
    public  <T> CacheConfiguration<CacheKey, T> produceCacheConfiguration(CacheMode cacheMode, CacheAtomicityMode atomicityMode, String cacheName)
    {
        final CacheConfiguration<CacheKey, T> cacheConfiguration = new CacheConfiguration<>(cacheName);

        cacheConfiguration.setCacheMode(cacheMode);

        cacheConfiguration.setAtomicityMode(atomicityMode);

        cacheConfiguration.setDataRegionName("Default_Region");
        
        cacheConfiguration.setIndexedTypes(CacheKey.class, ContractDTOSimple.class);
        cacheConfiguration.setKeyConfiguration(new CacheKeyConfiguration(CacheKey.class));
        
        cacheConfiguration.setBackups(0);

        cacheConfiguration.setCopyOnRead(false);

        cacheConfiguration.setEventsDisabled(true);

        return cacheConfiguration;

    }

    public IgniteConfiguration produceIgniteConfiguration(boolean withPersistence, PageReplacementMode pageReplacementMode, boolean writeThrottlingEnabled, int walSegmentSize, String walStorePath, String walArchivePath, int checkpointFreq, WALMode walMode, String storagePath, String workDir)
    {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(createDefaultDataRegionConfiguration(DataPageEvictionMode.DISABLED, 4L*1024*1024*1024));
        dataStorageConfiguration.setCheckpointReadLockTimeout(0);
        if (withPersistence)
        {
            configurePersistence(dataStorageConfiguration, pageReplacementMode, writeThrottlingEnabled, walSegmentSize, walStorePath, walArchivePath, checkpointFreq, walMode, storagePath);
        }

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();

        igniteConfiguration.setIgniteInstanceName("IgniteDataGrid");


        igniteConfiguration.setWorkDirectory(workDir);
        igniteConfiguration.setMetricsLogFrequency(0L);
        igniteConfiguration.setMetricExporterSpi(new JmxMetricExporterSpi());

        igniteConfiguration.setDiscoverySpi(new IsolatedDiscoverySpi());

        igniteConfiguration.setCommunicationSpi(new StandaloneNoopCommunicationSpi());

        igniteConfiguration.setDataStorageConfiguration(dataStorageConfiguration);
        
        SqlConfiguration c =  new SqlConfiguration();
       // c.setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        igniteConfiguration.setSqlConfiguration(c);
        return igniteConfiguration;
    }

    private void configurePersistence(DataStorageConfiguration dataStorageConfiguration, PageReplacementMode pageReplacementMode, boolean writeThrottlingEnabled, int walSegmentSize, String walStorePath, String walArchivePath, int checkpointFreq, WALMode logOnly, String storagePath)
    {
        dataStorageConfiguration.getDefaultDataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setPageReplacementMode(pageReplacementMode);

        dataStorageConfiguration
                .setWriteThrottlingEnabled(writeThrottlingEnabled)
                .setWalSegmentSize(walSegmentSize)
                .setWalPath(walStorePath)
                .setWalArchivePath(walArchivePath)
                .setCheckpointFrequency(checkpointFreq)
                .setWalMode(logOnly);

        if (storagePath != null && !storagePath.isEmpty())
        {
            dataStorageConfiguration.setStoragePath(storagePath);
        }
    }


    private DataRegionConfiguration createDefaultDataRegionConfiguration(DataPageEvictionMode dataPageEvictionMode, long maxSize)
    {
        return new DataRegionConfiguration()
                .setName("Default_Region")
                .setMetricsEnabled(false)
                .setPageEvictionMode(dataPageEvictionMode)
                .setMaxSize(maxSize);
    }

}
