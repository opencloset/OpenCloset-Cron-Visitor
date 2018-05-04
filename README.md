# OpenCloset-Cron-Visitor #

[![Build Status](https://travis-ci.org/opencloset/OpenCloset-Cron-Visitor.svg?branch=v0.3.5)](https://travis-ci.org/opencloset/OpenCloset-Cron-Visitor)

예약/방문/미방문/대여 수와 이벤트 수치를 통계용 table 인 visitor 에 넣어주는 cronjob

- 일일 온/오프라인 방문자수를 계산 (AM 00:05)
- 일일 취업날개 이벤트 방문/미방문수를 계산 (AM 00:07)
- 일일 linkstart 이벤트 방문/미방문수와 주문서 금액을 계산 (AM 00:08)
- 일일 관악고용센터 이벤트 방문/미방문수를 계산 (AM 00:09)
- 일일 십시일밥 이벤트 방문/미방문수를 계산 (AM 00:10)
- 일일 해피빈캠페인 이벤트 방문/미방문수를 계산 (AM 00:11)
- 일일 인천광역시 일자리정책과 이벤트 방문/미방문수를 계산 (AM 00:12)
- 일일 안양시 청년옷장 이벤트 방문/미방문수를 계산 (AM 00:13)
- 일일 한신대학교 이벤트 방문/미방문수를 계산 (AM 00:14)
- 일일 군포시 이벤트 방문/미방문수를 계산 (AM 00:15)

## Build docker image ##

    $ docker build -t opencloset/cron/visitor .
