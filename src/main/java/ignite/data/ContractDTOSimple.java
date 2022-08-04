/*
 * *************************************************************************
 * Copyright (C) Wolters Kluwer Financial Services. All rights reserved.
 *
 * This computer program is protected by copyright law and international
 * treaties. Unauthorized reproduction or distribution of this program,
 * or any portion of it, may result in severe civil and criminal penalties,
 * and will be prosecuted to the maximum extent possible under the law.
 * *************************************************************************
 */
package ignite.data;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class ContractDTOSimple
{

    @QuerySqlField(inlineSize = 200,  index = true)
    public Long dataGroupId;
    
    @QuerySqlField(index = true)
    public String softLinkKey;
	
	
}
