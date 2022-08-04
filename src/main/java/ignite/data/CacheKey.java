package ignite.data;

import java.io.Serializable;
import java.util.Objects;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public final class CacheKey implements Serializable
{
    private static final long serialVersionUID = -8460496823936658235L;

    @AffinityKeyMapped
    private final Long dataGroupId;
    private final String softLinkKey;

    public CacheKey(Long dataGroupId, String softLinkKey)
    {
        this.dataGroupId = dataGroupId;
        this.softLinkKey = softLinkKey;
    }

    public Long getDataGroupId()
    {
        return dataGroupId;
    }

    public String getSoftLinkKey()
    {
        return softLinkKey;
    }

    @Override
    public String toString()
    {
        return String.format("Key [%d, %s]", dataGroupId, softLinkKey);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        CacheKey cacheKey = (CacheKey) o;
        return dataGroupId.equals(cacheKey.dataGroupId) && softLinkKey.equals(cacheKey.softLinkKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataGroupId, softLinkKey);
    }
}
