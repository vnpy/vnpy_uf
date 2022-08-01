# VeighNa框架的恒生云UF2.0证券测试交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.1-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于恒生T2SDK接口封装开发的恒生云UF2.0证券测试环境交易接口，行情数据由TuShare数据服务提供。

## 安装

安装需要基于3.3.0版本的【[**VeighNa**](https://github.com/vnpy/vnpy)】和Python3.7环境。

直接使用pip命令：

```
pip install vnpy_uf
```


或者下载解压后在cmd中运行：

```
python setup.py install
```

## 使用

以脚本方式启动：

```
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_uf import UfGateway


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(UfGateway)
    
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
```

## 连接

请注意，连接前请在【全局配置】处配置好TuShare数据服务。
