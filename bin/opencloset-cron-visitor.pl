use utf8;
use strict;
use warnings;

use FindBin qw( $Script );
use Getopt::Long::Descriptive;

use DateTime;

use OpenCloset::Config;
use OpenCloset::Cron::Visitor
    qw/visitor_count event_wings event_linkstart event_gwanak event_10bob event_happybean event_incheonjob event_anyangyouth event_hanshin_univ/;
use OpenCloset::Cron::Worker;
use OpenCloset::Cron;
use OpenCloset::Schema;

my $config_file = shift;
die "Usage: $Script <config path>\n" unless $config_file && -f $config_file;

my $CONF     = OpenCloset::Config::load($config_file);
my $APP_CONF = $CONF->{$Script};
my $DB_CONF  = $CONF->{database};
my $TIMEZONE = $CONF->{timezone};

die "$config_file: $Script is needed\n"  unless $APP_CONF;
die "$config_file: database is needed\n" unless $DB_CONF;
die "$config_file: timezone is needed\n" unless $TIMEZONE;

my $DB = OpenCloset::Schema->connect(
    { dsn => $DB_CONF->{dsn}, user => $DB_CONF->{user}, password => $DB_CONF->{pass}, %{ $DB_CONF->{opts} }, } );

our %EVENT_MAP = (
    'seoul-2017' => 'wings',
);

sub _collect_event_stat_daily {
    my $name = shift;
    return unless $name;

    my $fn_name = $EVENT_MAP{$name} || $name;
    my $today = DateTime->today( time_zone => $TIMEZONE );
    my $date = $today->clone->subtract( days => 1 );
    my $fn = 'event_' . $fn_name;
    my $count;
    {
        no strict 'refs';
        $count = $fn->( $DB, $date );
    }

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
            my $count   = visitor_count( $DB, $date );
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
        name      => 'insert_event_wings_daily', # 일일 취업날개 방문자 수
        cron      => '07 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('seoul-2017');

            my $today = DateTime->today( time_zone => $TIMEZONE );
            my $date = $today->clone->subtract( days => 1 );

            for my $key (qw/offline online/) {
                my $online = $key eq 'online' ? 1 : 0;
                my $daily = $DB->resultset('Visitor')->search(
                    {
                        date   => "$date",
                        event  => 'seoul-2017',
                        online => $online,
                    },
                    { rows => 1 }
                )->single;

                next unless $daily;

                my $extra = $DB->resultset('Visitor')->search(
                    {
                        date   => "$date",
                        online => $online,
                        event  => 'extra-seoul-2017'
                    },
                    { rows => 1 }
                )->single;

                next unless $extra;

                my %columns = $daily->get_columns;
                my %extra   = $extra->get_columns;
                $daily->update(
                    {
                        visited        => $columns{visited} + $extra{visited},
                        visited_male   => $columns{visited_male} + $extra{visited_male},
                        visited_female => $columns{visited_female} + $extra{visited_female},
                        visited_age_10 => $columns{visited_age_10} + $extra{visited_age_10},
                        visited_age_20 => $columns{visited_age_20} + $extra{visited_age_20},
                        visited_age_30 => $columns{visited_age_30} + $extra{visited_age_30},
                    }
                );
            }
        }
    );
};

my $worker3 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_linkstart_daily', # 일일 linkstart 방문자 수
        cron      => '08 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );

            my $today = DateTime->today( time_zone => $TIMEZONE );
            my $date = $today->clone->subtract( days => 1 );
            my $count = event_linkstart( $DB, $date );

            for my $key (qw/offline online/) {
                my $stat = $count->{$key};
                my $online = $key eq 'online' ? 1 : 0;

                $DB->resultset('Visitor')->create(
                    {
                        date                     => "$date",
                        online                   => $online,
                        visited                  => $stat->{male}{visited} + $stat->{female}{visited},
                        visited_male             => $stat->{male}{visited},
                        visited_female           => $stat->{female}{visited},
                        visited_age_10           => $stat->{10}{visited},
                        visited_age_20           => $stat->{20}{visited},
                        visited_age_30           => $stat->{30}{visited},
                        visited_rate_30          => $stat->{rate_30}{visited},
                        visited_rate_30_sum      => $stat->{rate_30}{sum},
                        visited_rate_30_discount => $stat->{rate_30}{disstat},

                        unvisited        => $stat->{male}{unvisited} + $stat->{female}{unvisited},
                        unvisited_male   => $stat->{male}{unvisited},
                        unvisited_female => $stat->{female}{unvisited},
                        unvisited_age_10 => $stat->{10}{unvisited},
                        unvisited_age_20 => $stat->{20}{unvisited},
                        unvisited_age_30 => $stat->{30}{unvisited},

                        event => 'linkstart',
                    }
                );
            }
        }
    );
};

my $worker4 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_gwanak_daily', # 일일 관악고용센터 방문자 수
        cron      => '09 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('gwanak');
        }
    );
};

my $worker5 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_10bob_daily', # 일일 십시일밥 방문자 수
        cron      => '10 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('10bob');
        }
    );
};

my $worker6 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_happybean_daily', # 일일 해피빈캠페인 방문자 수
        cron      => '11 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('happybean');
        }
    );
};

my $worker7 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_incheonjob_daily', # 일일 인천광역시 일자리정책과 방문자 수
        cron      => '12 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('incheonjob');
        }
    );
};

my $worker8 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_anyangyouth_daily', # 일일 안양시 청년옷장 방문자 수
        cron      => '13 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('anyangyouth');
        }
    );
};

my $worker9 = do {
    my $w;
    $w = OpenCloset::Cron::Worker->new(
        name      => 'insert_event_hanshin_univ_daily', # 일일 한신대학교 방문자 수
        cron      => '14 00 * * *',
        time_zone => $TIMEZONE,
        cb        => sub {
            my $name = $w->name;
            my $cron = $w->cron;
            AE::log( info => "$name\[$cron] launched" );
            _collect_event_stat_daily('hanshin_univ');
        }
    );
};

my $cron = OpenCloset::Cron->new(
    aelog   => $APP_CONF->{aelog},
    port    => $APP_CONF->{port},
    delay   => $APP_CONF->{delay},
    workers => [ $worker1, $worker2, $worker3, $worker4, $worker5, $worker6, $worker7, $worker8, $worker9 ],
);

$cron->start;
