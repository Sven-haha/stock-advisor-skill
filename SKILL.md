---
name: A股操盘手
description: 每个交易日自动执行集合竞价分析、盘中异动监控及下午策略建议，覆盖大洋电机、阳光电源、胜宏科技、航天电子、科德教育、永杉锂业。
version: 2.0.0
author: 您的GitHub用户名
runtime:
  language: python3
  entry: stock_advisor.py
  args: []
schedule:
  - cron: "25 9 * * 1-5"   # 周一至周五 9:25
  - cron: "0 13 * * 1-5"    # 周一至周五 13:00
  - cron: "10 13 * * 1-5"   # 周一至周五 13:10
environment:
  - name: STOCK_CONFIG
    value: "stock_config.json"
---

# A股操盘手技能

## 功能
- 每日9:25 集合竞价分析
- 盘中实时监控快速拉升（5分钟涨幅>2%且量比>1.5）并推送建议
- 下午13:00、13:10 发布下午操盘策略
- 支持通过 `stock_config.json` 动态更新股票池

## 使用方法
1. 将本仓库导入 Qclaw 平台
2. 确保 Qclaw 环境已安装 Python 依赖：`pip install akshare pandas numpy`
3. 在 Qclaw 中设置定时任务（已内置在 skill.yaml 中）

## 自定义股票池
修改 `stock_config.json` 文件（格式见示例），保存后重启技能即可生效。

## 注意事项
- 本工具基于公开数据，仅供参考，不构成投资建议。
- 若数据源 akshare 出现异常，请检查网络或更换数据接口。
