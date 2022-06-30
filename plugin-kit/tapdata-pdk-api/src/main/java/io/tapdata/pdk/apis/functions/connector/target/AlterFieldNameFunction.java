package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface AlterFieldNameFunction {
    void alterFieldName(TapConnectorContext connectorContext, TapAlterFieldNameEvent alterFieldNameEvent) throws Throwable;
}
