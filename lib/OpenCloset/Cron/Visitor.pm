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

    my $rs = $schema->resultset('Order')->search(
        {
            'me.status_id' => { 'not in' => [ $NOT_VISITED, $RESERVATED ] },
            'coupon.status' => { -in => [ 'used', 'reserved', 'provided' ] },
        },
        { join => [qw/booking coupon/] }
    )->search_literal( 'DATE(`booking`.`date`) = ?', $date->ymd );

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

    my $year = DateTime->now->year;
    while ( my $order = $rs->next ) {
        my $user      = $order->user;
        my $user_info = $user->user_info;
        next unless $user_info;

        my $gender = $user_info->gender;
        next unless $gender;

        my $birth = $user_info->birth;
        my $age = int( ( $year - $birth ) / 10 ) * 10;

        my $coupon = $order->coupon;
        my $coupon_status = $coupon->status || '';
        if ( $coupon_status eq 'used' ) {
            ++$visitor{$gender}{visited};
            ++$visitor{$age}{visited};
        }
        else {
            ++$visitor{$gender}{unvisited};
            ++$visitor{$age}{unvisited};
        }
    }

    return \%visitor;
}

1;

=head1 COPYRIGHT and LICENSE

The MIT License (MIT)

Copyright (c) 2017 열린옷장

=cut
