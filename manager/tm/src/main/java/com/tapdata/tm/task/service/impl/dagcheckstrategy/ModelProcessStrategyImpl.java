package com.tapdata.tm.task.service.impl.dagcheckstrategy;

import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.constant.DagOutputTemplate;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.DagLogStrategy;
import com.tapdata.tm.task.service.TaskDagService;
import com.tapdata.tm.utils.Lists;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;

@Component("modelProcessStrategy")
@Setter(onMethod_ = {@Autowired})
public class ModelProcessStrategyImpl implements DagLogStrategy {

    private TaskDagService taskDagService;

    private final DagOutputTemplateEnum templateEnum = DagOutputTemplateEnum.MODEL_PROCESS_CHECK;

    @Override
    public List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail) {
        List<TaskDagCheckLog> result = Lists.newArrayList();

        String taskId = taskDto.getId().toHexString();
        LinkedList<DatabaseNode> sourceNode = taskDto.getDag().getSourceNode();
        int total = sourceNode.getFirst().getTableNames().size();

        BigDecimal time = new BigDecimal(total).divide(new BigDecimal(50), 1, RoundingMode.HALF_UP);

        TaskDagCheckLog preLog = new TaskDagCheckLog();
        String preContent = MessageFormat.format(DagOutputTemplate.MODEL_PROCESS_INFO_PRELOG, total, time);
        preLog.setTaskId(taskId);
        preLog.setCheckType(templateEnum.name());
        preLog.setCreateAt(DateUtil.date());
        preLog.setCreateUser(userDetail.getUserId());
        preLog.setLog(preContent);
        preLog.setGrade(Level.INFO.getValue());
        result.add(preLog);

        int dagHash = taskDagService.calculationDagHash(taskDto);

        if (dagHash == taskDto.getTransformDagHash() && taskDto.getTransformed()) {
            if (CollectionUtils.isNotEmpty(sourceNode)) {

                String content = MessageFormat.format(templateEnum.getInfoTemplate(), DateUtil.now(), total, total);

                TaskDagCheckLog log = new TaskDagCheckLog();
                log.setTaskId(taskId);
                log.setCheckType(templateEnum.name());
                log.setCreateAt(DateUtil.date());
                log.setCreateUser(userDetail.getUserId());
                log.setLog(content);
                log.setGrade(Level.INFO.getValue());

                result.add(log);
                return result;
            }

        }
        return result;
    }
}
