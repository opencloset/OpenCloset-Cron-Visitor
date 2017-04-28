package OpenCloset::Cron::Visitor;

require Exporter;
@ISA       = qw/Exporter/;
@EXPORT_OK = qw/visitor_count visitor_count_online event_wings event_linkstart/;

use OpenCloset::Constants::Status qw/
    $NOT_VISITED
    $RESERVATED
    $RENTAL
    $RENTABLE
    $CHOOSE_CLOTHES
    $CHOOSE_ADDRESS
    $PAYMENT
    $PAYMENT_DONE
    $WAITING_DEPOSIT
    $PAYBACK/;

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

    my $rs = $schema->resultset('Order')->search( { online => 0 }, { join => 'booking' } )
        ->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    my %visitor = (
        male   => { reserved => 0, visited => 0, unvisited => 0, rented => 0, bestfit => 0, },
        female => { reserved => 0, visited => 0, unvisited => 0, rented => 0, bestfit => 0, },
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

=head2 visitor_count_online( $schema, $date )

온라인 일별 대여자 수

=cut

sub visitor_count_online {
    my ( $schema, $date ) = @_;
    return unless $date;

    my $from = $date->clone->truncate( to => 'day' );
    my $to = $from->clone;
    $to->set( hour => 23, minute => 59, second => 59 );
    my $rs = $schema->resultset('Order')->search(
        {
            online      => 1,
            rental_date => { -between => [ $from->datetime(), $to->datetime() ] }
        },
        undef
    );

    my %visitor = (
        male   => { rented => 0 },
        female => { rented => 0 },
    );

    while ( my $order = $rs->next ) {
        my $user      = $order->user;
        my $user_info = $user->user_info;
        next unless $user_info;

        my $gender = $user_info->gender;
        next unless $gender;

        ++$visitor{$gender}{rented};
    }

    return \%visitor;
}

=head2 event_wings( $schema, $date )

취업날개 일별 방문자 수

=cut

sub event_wings {
    my ( $schema, $date ) = @_;
    return unless $date;

    my %visitor = (
        male   => { visited => 0, unvisited => 0, },
        female => { visited => 0, unvisited => 0, },
        10     => { visited => 0, unvisited => 0, },
        20     => { visited => 0, unvisited => 0, },
        30     => { visited => 0, unvisited => 0, },
    );

    my $year = $date->year;
    my $rs   = $schema->resultset('Order')->search(
        {
            'me.status_id'  => { 'not in' => [ $NOT_VISITED, $RESERVATED ] },
            'coupon.status' => 'used',
            'coupon.desc' => { -like => 'seoul-2017%' },
        },
        {
            select => [
                'user_info.gender',
                'user_info.birth',
            ],
            as => [
                'gender',
                'birth'
            ],
            join => [ 'booking', 'coupon', { user => 'user_info' } ]
        }
    )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    while ( my $row = $rs->next ) {
        my $gender = $row->get_column('gender');
        my $birth  = $row->get_column('birth');
        next unless $gender;
        next unless $birth;

        $visitor{$gender}{visited}++;

        my $age = int( ( $year - $birth ) / 10 ) * 10;
        $visitor{$age}{visited}++;
    }

    $rs = $schema->resultset('Order')->search(
        {
            'me.status_id'  => { -in => [ $NOT_VISITED, $RESERVATED ] },
            'coupon.status' => 'reserved',
            'coupon.desc' => { -like => 'seoul-2017%' },
        },
        {
            select => [
                'user_info.gender',
                'user_info.birth',
            ],
            as => [
                'gender',
                'birth'
            ],
            join => [ 'booking', 'coupon', { user => 'user_info' } ]
        }
    )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    while ( my $row = $rs->next ) {
        my $gender = $row->get_column('gender');
        my $birth  = $row->get_column('birth');
        next unless $gender;
        next unless $birth;

        $visitor{$gender}{unvisited}++;

        my $age = int( ( $year - $birth ) / 10 ) * 10;
        $visitor{$age}{unvisited}++;
    }

    return \%visitor;
}

=head2 event_linkstart( $schema, $date )

LINKStart 일별 방문자 수와 금액

=cut

sub event_linkstart {
    my ( $schema, $date ) = @_;
    return unless $date;

    my %visitor = (
        male    => { visited => 0, unvisited => 0, },
        female  => { visited => 0, unvisited => 0, },
        10      => { visited => 0, unvisited => 0, },
        20      => { visited => 0, unvisited => 0, },
        30      => { visited => 0, unvisited => 0, },
        rate_30 => { sum     => 0, discount  => 0 },
    );

    my $year = $date->year;
    my $rs   = $schema->resultset('Order')->search(
        {
            'me.status_id'  => { 'not in' => [ $NOT_VISITED, $RESERVATED ] },
            'coupon.status' => 'used',
        },
        {
            select => [
                'me.id',
                'user_info.gender',
                'user_info.birth',
            ],
            as => [
                'id',
                'gender',
                'birth'
            ],
            join => [ 'booking', 'coupon', { user => 'user_info' } ]
        }
    )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    while ( my $row = $rs->next ) {
        my $id     = $row->get_column('id');
        my $gender = $row->get_column('gender');
        my $birth  = $row->get_column('birth');

        my $order = $schema->resultset('Order')->find( { id => $id } );
        my $final_price = 0;

        if ( $order->online ) {
            my $status_id = $order->status_id;
            if ( "$CHOOSE_CLOTHES $CHOOSE_ADDRESS $PAYMENT $PAYMENT_DONE $WAITING_DEPOSIT $PAYBACK" =~ m/\b$status_id\b/ ) {
                my $details = $order->order_details;
                while ( my $detail = $details->next ) {
                    my $name = $detail->name;
                    next unless $name =~ m/^[a-z]/;

                    $final_price += $detail->final_price;
                }
            }
            else {
                my $details = $order->order_details( { clothes_code => { '!=' => undef } } );
                while ( my $detail = $details->next ) {
                    $final_price += $detail->final_price;
                }
            }
        }
        else {
            my $details = $order->order_details( { clothes_code => { '!=' => undef } } );
            while ( my $od = $details->next ) {
                $final_price += $od->final_price;
            }
        }

        $visitor{rate_30}{sum} += $final_price;
        my $detail = $order->order_details( { name => '30% 할인쿠폰' }, { rows => 1 } )->single;
        next unless $detail;

        $visitor{rate_30}{discount} += $detail->final_price * -1;

        next unless $gender;
        next unless $birth;

        $visitor{$gender}{visited}++;

        my $age = int( ( $year - $birth ) / 10 ) * 10;
        $visitor{$age}{visited}++;
    }

    $rs = $schema->resultset('Order')->search(
        {
            'me.status_id'  => { -in => [ $NOT_VISITED, $RESERVATED ] },
            'coupon.status' => 'reserved',
        },
        {
            select => [
                'user_info.gender',
                'user_info.birth',
            ],
            as => [
                'gender',
                'birth'
            ],
            join => [ 'booking', 'coupon', { user => 'user_info' } ]
        }
    )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

    while ( my $row = $rs->next ) {
        my $gender = $row->get_column('gender');
        my $birth  = $row->get_column('birth');
        next unless $gender;
        next unless $birth;

        $visitor{$gender}{unvisited}++;

        my $age = int( ( $year - $birth ) / 10 ) * 10;
        $visitor{$age}{unvisited}++;
    }

    return \%visitor;
}

1;

=head1 COPYRIGHT and LICENSE

The MIT License (MIT)

Copyright (c) 2017 열린옷장

=cut
