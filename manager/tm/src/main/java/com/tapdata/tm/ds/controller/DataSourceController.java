package com.tapdata.tm.ds.controller;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.bean.NoSchemaFilter;
import com.tapdata.tm.ds.dto.UpdateTagsDto;
import com.tapdata.tm.ds.param.ValidateTableParam;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.ds.vo.AllDataSourceConnectionVo;
import com.tapdata.tm.ds.vo.ValidateTableVo;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import lombok.Setter;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/8/19
 * @Description: 数据源连接相关接口
 */
@RestController
@CrossOrigin("*")
@Tag(name = "Connections", description = "数据源连接相关接口")
@RequestMapping({"api/Connections", "/api/v2/ds"})
@Setter(onMethod_ = {@Autowired})
public class DataSourceController extends BaseController {

    private DataSourceService dataSourceService;

    private MetadataDefinitionService metadataDefinitionService;

    /**
     * 添加数据源连接
     *
     * @param connection 数据源连接实体
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "添加数据源连接")
    @PostMapping
    public ResponseMessage<DataSourceConnectionDto> add(@RequestBody DataSourceConnectionDto connection) {
        connection.setId(null);

        return success(dataSourceService.add(connection, getLoginUser()));
    }

    /**
     * 修改数据源
     *
     * @param updateDto 修改后的名称
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "修改数据源连接")
    @PatchMapping
    public ResponseMessage<DataSourceConnectionDto> update(@RequestBody DataSourceConnectionDto updateDto) {
        return success(dataSourceService.update(getLoginUser(), updateDto));
    }

    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping
    public ResponseMessage<Page<DataSourceConnectionDto>> find( @RequestParam(value = "filter", required = false) String filterJson,
                                                                @RequestParam(value = "noSchema", required = false) Boolean noSchema) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }

        if (noSchema == null) {
            noSchema = true;
            if (filter.getNoSchema() != null) {
                noSchema = filter.getNoSchema() == 1;
            } else if (filter.getFields() != null && filter.getFields().size() != 0) {
                Object schema = filter.getFields().get("schema");
                noSchema = (schema == null);
            }
        }

        //隐藏密码
        Page<DataSourceConnectionDto> dataSourceConnectionDtoPage = dataSourceService.list(filter, noSchema, getLoginUser());

        return success(dataSourceConnectionDtoPage);
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表,不分页")
    @GetMapping("findAll")
    public ResponseMessage<List<AllDataSourceConnectionVo>> findAll(@RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }

        //隐藏密码
        List<DataSourceConnectionDto> dataSourceConnectionDtoList = dataSourceService.findAll(filter.getWhere(), getLoginUser());
        List<AllDataSourceConnectionVo> allDataSourceConnectionVoList = BeanUtil.deepCloneList(dataSourceConnectionDtoList, AllDataSourceConnectionVo.class);

        return success(allDataSourceConnectionVoList);
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据条件查询数据源连接列表,不分页")
    @GetMapping("listAll")
    public ResponseMessage<List<DataSourceConnectionDto>> logCollectorInfo(@RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }

        return success(dataSourceService.listAll(filter, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<DataSourceConnectionDto> put(@RequestBody DataSourceConnectionDto dataSource) {
        return success(dataSourceService.replaceOrInsert(dataSource, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = dataSourceService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }


    /**
     * 测试连接时，以及修改属性，都调用该方法
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> updateById(@PathVariable("id") String id, @RequestBody(required = false) DataSourceConnectionDto dataSource) {
        if (dataSource == null) {
            dataSource = new DataSourceConnectionDto();
        }
        dataSource.setId(MongoUtils.toObjectId(id));
        return success(dataSourceService.update(getLoginUser(), dataSource));
    }

    /**
     * 给数据源连接修改资源分类
     *
     * @param updateTagsDto 数据源连接id列表
     * @return DataSourceConnectionDto
     */
    @Deprecated
    @Operation(summary = "给数据源连接修改资源分类")
    @PatchMapping("batchUpdateListtag")
    public ResponseMessage<DataSourceConnectionDto> updateTag(@RequestBody UpdateTagsDto updateTagsDto) {
        dataSourceService.updateTag(getLoginUser(), updateTagsDto);
        return success();
    }

    /**
     * 根据id查询数据源连接，需要判断用户id
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据id查询数据源连接")
    @GetMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> findById(@PathVariable("id") String id, @RequestParam(value = "fields", required = false) String fieldsJson,
                                                             @RequestParam(value = "noSchema", required = false) String noSchema) {
        Field fields = parseField(fieldsJson);

        boolean no = true;
        if (noSchema != null) {
            no = ("1".equals(noSchema) || "true".equals(noSchema));
        }
        return success(dataSourceService.getById(toObjectId(id), fields, no, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<DataSourceConnectionDto> replaceById(@PathVariable("id") String id, @RequestBody DataSourceConnectionDto dataSource) {
        UserDetail user = getLoginUser();
        Boolean submit = dataSource.getSubmit();
        dataSourceService.updateCheck(user, dataSource);
        DataSourceConnectionDto connectionDto = dataSourceService.replaceById(toObjectId(id), dataSource, getLoginUser());
        dataSourceService.updateAfter(user, connectionDto, submit);
        return success(connectionDto);
    }


    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataSource dataSource
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<DataSourceConnectionDto> replaceById2(@PathVariable("id") String id, @RequestBody DataSourceConnectionDto dataSource) {
        UserDetail user = getLoginUser();
        Boolean submit = dataSource.getSubmit();
        dataSourceService.updateCheck(user, dataSource);
        DataSourceConnectionDto connectionDto = dataSourceService.replaceById(toObjectId(id), dataSource, user);
        dataSourceService.updateAfter(user, connectionDto, submit);
        return success(connectionDto);
    }


    /**
     * 删除数据源连接
     *
     * @param id id
     */
    @Operation(summary = "删除数据源连接")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        dataSourceService.delete(getLoginUser(), id);
        return success();
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @param id id
     * @return map
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = dataSourceService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }


    /**
     * 根据id查询数据源连接，需要判断用户id
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "根据id查询数据源连接")
    @GetMapping("{id}/customQuery")
    public ResponseMessage<DataSourceConnectionDto> customQuery(@PathVariable("id") String id,
                                                                @RequestParam(value = "tableName", required = false) String tableName,
                                                                @RequestParam(value = "schema", required = false) Boolean schema) {
        return success(dataSourceService.customQuery(toObjectId(id), tableName, schema, getLoginUser()));
    }


    /**
     * 根据条件查询数据源连接列表
     *
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "根据条件查询数据源连接列表")
    @GetMapping("count")
    public ResponseMessage<Map<String, Long>> count(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "where", required = false) String whereJson) {
        Where where = parseWhere(whereJson);
        long count = dataSourceService.count(where, getLoginUser());


        HashMap<String, Long> returnMap = new HashMap<>();
        returnMap.put("count", count);

        return success(returnMap);
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson filterJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<DataSourceConnectionDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        filterJson = replaceLoopBack(filterJson);
        NoSchemaFilter filter = JsonUtil.parseJson(filterJson, NoSchemaFilter.class);
        if (filter == null) {
            filter = new NoSchemaFilter();
        }


        boolean noSchema = true;
        if (filter.getNoSchema() != null) {
            noSchema = filter.getNoSchema() == 1;
        } else if (filter.getFields() != null && filter.getFields().size() != 0) {
            Object schema = filter.getFields().get("schema");
            noSchema = (schema == null);
        }

        return success(dataSourceService.findOne(filter, getLoginUser(), noSchema));
    }

    /**
     * 复制数据源连接
     *
     * @param id id
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "复制数据源")
    @PostMapping("{id}/copy")
    public ResponseMessage<DataSourceConnectionDto> copy(@PathVariable("id") String id) {
        return success(dataSourceService.copy(getLoginUser(), id));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson whereJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
        Where where = parseWhere(whereJson);

        long count;
        UserDetail user = getLoginUser();
        if (reqBody.indexOf("\"$set\"") > 0 || reqBody.indexOf("\"$setOnInsert\"") > 0 || reqBody.indexOf("\"$unset\"") > 0) {
            Document updateDto = InstanceFactory.instance(JsonParser.class).fromJson(reqBody, Document.class);
            count = dataSourceService.upsertByWhere(where, updateDto, null, user);
        } else {
            DataSourceConnectionDto connectionDto = JsonUtil.parseJsonUseJackson(reqBody, DataSourceConnectionDto.class);
            count = dataSourceService.upsertByWhere(where, null, connectionDto, user);
        }
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }


    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson whereJson
     * @return DataSourceConnectionDto
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<DataSourceConnectionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody DataSourceConnectionDto dataSource) {
        Where where = parseWhere(whereJson);
        return success(dataSourceService.upsertByWhere(where, dataSource, getLoginUser()));
    }


    @GetMapping("distinct/{field}")
    public ResponseMessage<List<String>> distinct(@PathVariable String field) {
        return success(dataSourceService.distinct(field, getLoginUser()));
    }

    @GetMapping("databaseType")
    public ResponseMessage<List<String>> databaseType() {
        return success(dataSourceService.databaseType(getLoginUser()));
    }

    /**
     * 校验表是否属于某个数据库
     */
    @Operation(summary = " 校验表是否属于某个数据库")
    @PostMapping("validateTable")
    public ResponseMessage<ValidateTableVo> validateTable(@RequestBody ValidateTableParam validateTableParam) {
        return success(dataSourceService.validateTable(validateTableParam.getConnectionId(), validateTableParam.getTableList()));
    }

    /**
     *
     */
    @Operation(summary = " 批量修改所属类别")
    @PatchMapping("batchUpdateListtags")
    public ResponseMessage<List<String>> batchUpdateListTags(@RequestBody BatchUpdateParam batchUpdateParam) {
        return success(metadataDefinitionService.batchUpdateListTags("Connections", batchUpdateParam, getLoginUser()));
    }

    /**
     * 现在除了 kafka, elasticsearch以外  都支持数据校验
     */
    @Operation(summary = "数据源支持功能列表")
    @GetMapping("supportList")
    public ResponseMessage<List> supportList() {
        return success(dataSourceService.supportList(getLoginUser()));
    }


}
