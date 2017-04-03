package OpenCloset::Cron::Visitor;

require Exporter;
@ISA       = qw/Exporter/;
@EXPORT_OK = qw/visitor_count event_wings_count/;

use OpenCloset::Constants::Status qw/$NOT_VISITED $RESERVATED/;

use strict;
use warnings;

=encoding utf8

=head1 NAME

OpenCloset::Cron::Visitor - 방문자수와 관려된 cronjob

=head1 SYNOPSIS

    perl bin/opencloset-cron-visitor.pl /path/to/app.conf

=head1 DESCRIPTION

=over

=item *

일일 방문자수를 계산 (AM 00:05)

=item *

일일 취업날개 이벤트 방문/미방문수를 계산 (AM 00:10)

=back

=head1 METHODS

=head2 visitor_count( $schema, $date )

일별 방문자 수

=cut

sub visitor_count {
    my ( $schema, $date ) = @_;
    return unless $date;

    my $rs = $schema->resultset('Order')->search( undef, { join => 'booking' } )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    my %visitor = (
        male => {
            reserved  => 0,
            visited   => 0,
            unvisited => 0,
            rented    => 0,
            bestfit   => 0,
        },
        female => {
            reserved  => 0,
            visited   => 0,
            unvisited => 0,
            rented    => 0,
            bestfit   => 0,
        },
    );

    while ( my $order = $rs->next ) {
        my $user      = $order->user;
        my $user_info = $user->user_info;
        next unless $user_info;

        my $gender = $user_info->gender;
        next unless $gender;

        ++$visitor{$gender}{reserved};
        ++$visitor{$gender}{rented} if $order->rental_date;
        if ( $order->status_id =~ m/^1[24]$/ ) {
            ++$visitor{$gender}{unvisited};
        }
        else {
            ++$visitor{$gender}{visited};
        }

        if ( $order->bestfit ) {
            ++$visitor{$gender}{bestfit};
        }
    }

    return \%visitor;
}

=head2 event_wings_count( $schema, $date )

취업날개 일별 방문자 수

=cut

sub event_wings_count {
    my ( $schema, $date ) = @_;
    return unless $date;

    my $storage = $schema->storage;
    my $sth = $storage->dbh_do(
        sub {
            my ( $storage, $dbh, @args ) = @_;
            my $sql = q{
                SELECT
                    *
                FROM
                    (
                        SELECT
                            a.`date`                                        AS 'booking_date'
                            ,b.`id`                                         AS 'order_id'
                            ,YEAR(a.`date`)                                 AS 'booking_year'
                            ,MONTH(a.`date`)                                AS 'booking_month'
                            ,DAY(a.`date`)                                  AS 'booking_day'
                            ,IF(b.status_id = 12 OR b.status_id = 14, 0, 1) AS 'is_visit'
                            ,d.gender                                       AS 'gender'
                            ,d.birth                                        AS 'birth'
                            ,YEAR(NOW()) - d.birth                          AS 'age'
                            ,TRUNCATE(YEAR(NOW()) - d.birth, -1)            AS 'age_group'
                            ,IF(b.`coupon_id` IS NOT NULL, 1, 0)            AS 'is_coupon_use'
                            ,e.`id`                                         AS 'coupon_id'
                            ,e.`update_date`                                AS 'coupon_date'
                            ,e.`status`                                     AS 'coupon_status'
                            ,SUBSTRING_INDEX(e.desc, '|', 1)                AS 'coupon_type'
                            ,IFNULL(a.`date` - e.`update_date`,0)           AS 'booking_coupon_issue_diff'
                        FROM
                            `booking` AS a
                            INNER JOIN `order`      AS b ON ( a.id = b.booking_id )
                            INNER JOIN `user`       AS c ON ( b.user_id = c.id )
                            INNER JOIN `user_info`  AS d ON ( c.id = d.user_id )
                            LEFT  JOIN `coupon`     AS e ON ( b.coupon_id = e.id )
                    ) AS x
                WHERE
                    `booking_coupon_issue_diff` >= -21600
                    AND DATE(`booking_date`) = ?
                };

            my $sth = $dbh->prepare($sql);
            my $rv = $sth->execute($date->ymd);

            return $sth;
        }
    );

    my %visitor = (
        male => {
            visited   => 0,
            unvisited => 0,
        },
        female => {
            visited   => 0,
            unvisited => 0,
        },
        10 => {
            visited   => 0,
            unvisited => 0,
        },
        20 => {
            visited   => 0,
            unvisited => 0,
        },
        30 => {
            visited   => 0,
            unvisited => 0,
        },
    );

    while(my $data = $sth->fetchrow_hashref ) {
        my $label = $data->{is_coupon_use} == 1 ? 'visited' : 'unvisited';

        $visitor{$data->{gender}}{$label}++;
        $visitor{$data->{age_group}}{$label}++;
    }

    return \%visitor;
}

1;

=head1 COPYRIGHT and LICENSE

The MIT License (MIT)

Copyright (c) 2017 열린옷장

=cut
