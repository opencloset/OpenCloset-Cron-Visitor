use utf8;
use strict;
use warnings;

use FindBin qw( $Script );
use Getopt::Long::Descriptive;

use DateTime;

use OpenCloset::Config;
use OpenCloset::Cron::Visitor qw/visitor_count/;
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
    {
        dsn      => $DB_CONF->{dsn},
        user     => $DB_CONF->{user},
        password => $DB_CONF->{pass},
        %{ $DB_CONF->{opts} },
    }
);

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
            my $count = visitor_count( $DB, $date );

            $DB->resultset('Visitor')->create(
                {
                    date            => "$date",
                    reserved        => $count->{male}{reserved} + $count->{female}{reserved},
                    reserved_male   => $count->{male}{reserved},
                    reserved_female => $count->{female}{reserved},

                    visited        => $count->{male}{visited} + $count->{female}{visited},
                    visited_male   => $count->{male}{visited},
                    visited_female => $count->{female}{visited},

                    unvisited        => $count->{male}{unvisited} + $count->{female}{unvisited},
                    unvisited_male   => $count->{male}{unvisited},
                    unvisited_female => $count->{female}{unvisited},

                    rented        => $count->{male}{rented} + $count->{female}{rented},
                    rented_male   => $count->{male}{rented},
                    rented_female => $count->{female}{rented},

                    bestfit        => $count->{male}{bestfit} + $count->{female}{bestfit},
                    bestfit_male   => $count->{male}{bestfit},
                    bestfit_female => $count->{female}{bestfit},
                }
            );
        }
    );
};

my $cron = OpenCloset::Cron->new(
    aelog   => $APP_CONF->{aelog},
    port    => $APP_CONF->{port},
    delay   => $APP_CONF->{delay},
    workers => [$worker1],
);

$cron->start;
