#!/usr/bin/env perl
use utf8;
use strict;
use warnings;

use FindBin qw( $Script );
use Getopt::Long::Descriptive;

use DateTime;

use OpenCloset::Config;
use OpenCloset::Cron::Visitor;
use OpenCloset::Cron::Worker;
use OpenCloset::Cron;
use OpenCloset::Schema;

my $config_file = shift;
die "Usage: $Script <config path>\n" unless $config_file && -f $config_file;

my $occ      = OpenCloset::Config->new( file => $config_file );
my $APP_CONF = $occ->conf->{$Script};
my $TIMEZONE = $occ->timezone;

die "$config_file: $Script is needed\n"  unless $APP_CONF;
die "$config_file: database is needed\n" unless $occ->dbic;
die "$config_file: timezone is needed\n" unless $TIMEZONE;

my $DB = OpenCloset::Schema->connect( $occ->dbic );

sub _collect_event_stat_daily {
    my $name = shift;
    return unless $name;

    my $today = DateTime->today( time_zone => $TIMEZONE );
    my $date = $today->clone->subtract( days => 1 );
    my $count;

    $count = OpenCloset::Cron::Visitor::event_common($DB, $date, $name);
    for my $key (qw/offline online/) {
        my $stat = $count->{$key};
        my $online = $key eq 'online' ? 1 : 0;
        $DB->resultset('Visitor')->create(
            {
                date           => "$date",
                online         => $online,
                visited        => $stat->{male}{visited} + $stat->{female}{visited},
                visited_male   => $stat->{male}{visited},
                visited_female => $stat->{female}{visited},
                visited_age_10 => $stat->{10}{visited},
                visited_age_20 => $stat->{20}{visited},
                visited_age_30 => $stat->{30}{visited},

                unvisited        => $stat->{male}{unvisited} + $stat->{female}{unvisited},
                unvisited_male   => $stat->{male}{unvisited},
                unvisited_female => $stat->{female}{unvisited},
                unvisited_age_10 => $stat->{10}{unvisited},
                unvisited_age_20 => $stat->{20}{unvisited},
                unvisited_age_30 => $stat->{30}{unvisited},

                event => $name,
            }
        );
    }
}

my $worker1 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_visitor_daily', # 일일 방문자 수
        cron      => '05 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );

            my $today = DateTime->today( time_zone => $TIMEZONE );
            my $date = $today->clone->subtract( days => 1 );
            my $count   = OpenCloset::Cron::Visitor::visitor_count( $DB, $date );
            my $offline = $count->{offline};
            my $online  = $count->{online};

            ## offline stat
            $DB->resultset('Visitor')->create(
                {
                    date            => "$date",
                    reserved        => $offline->{male}{reserved} + $offline->{female}{reserved},
                    reserved_male   => $offline->{male}{reserved},
                    reserved_female => $offline->{female}{reserved},

                    visited        => $offline->{male}{visited} + $offline->{female}{visited},
                    visited_male   => $offline->{male}{visited},
                    visited_female => $offline->{female}{visited},

                    unvisited        => $offline->{male}{unvisited} + $offline->{female}{unvisited},
                    unvisited_male   => $offline->{male}{unvisited},
                    unvisited_female => $offline->{female}{unvisited},

                    rented        => $offline->{male}{rented} + $offline->{female}{rented},
                    rented_male   => $offline->{male}{rented},
                    rented_female => $offline->{female}{rented},

                    bestfit        => $offline->{male}{bestfit} + $offline->{female}{bestfit},
                    bestfit_male   => $offline->{male}{bestfit},
                    bestfit_female => $offline->{female}{bestfit},
                }
            );

            ## online stat
            $DB->resultset('Visitor')->create(
                {
                    date   => "$date",
                    online => 1,

                    rented        => $online->{male}{rented} + $online->{female}{rented},
                    rented_male   => $online->{male}{rented},
                    rented_female => $online->{female}{rented},
                }
            );
        }
    );
};

my $worker2 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_allevent_daily', # 각 이벤트별 일일 방문 숫자
        cron      => '10 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            my $today = DateTime->today( time_zone => $TIMEZONE );
            my $events = $DB->resultset('Event')->search({
                start_date          => { '<=' => "$today" },
                end_date            => { '>=' => "$today" },
                'event_type.domain' => 'rental'
            }, {
                join => ['event_type']
            });

            while (my $event = $events->next) {
                _collect_event_stat_daily($event->name);
            }
        }
    );
};

my $cron = OpenCloset::Cron->new(
    aelog   => $APP_CONF->{aelog},
    port    => $APP_CONF->{port},
    delay   => $APP_CONF->{delay},
    workers => [
        $worker1,
        $worker2,
    ],
);

$cron->start;
