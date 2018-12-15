# OpenCloset-Cron-Visitor #

[![Build Status](https://travis-ci.org/opencloset/OpenCloset-Cron-Visitor.svg?branch=v0.3.9)](https://travis-ci.org/opencloset/OpenCloset-Cron-Visitor)

예약/방문/미방문/대여 수와 이벤트 수치를 통계용 table 인 visitor 에 넣어주는 cronjob

- 일일 온/오프라인 방문자수를 계산 (AM 00:05)
- 일일 취업날개 이벤트 방문/미방문수를 계산 (AM 00:07)
- 일일 이벤트 방문/미방문수를 계산 (AM 00:10)

## Build docker image ##

    $ docker build -t opencloset/cron/visitor .
