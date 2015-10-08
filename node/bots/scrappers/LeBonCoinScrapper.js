var xray = require('x-ray');
var async = require('async');
var moment = require('moment');
moment.locale('fr');

var LeBonCoinScrapper = {};
module.exports = LeBonCoinScrapper;

LeBonCoinScrapper.MAX_INDEX_PAGES = 10;

LeBonCoinScrapper.run = function run(options, progress, complete) {

    options = options || {};
    options.stopAtTimestamp = options.stopAtTimestamp || moment().subtract(60, 'minutes');

    getIndex(function (index) {
        var annonces = [];
        var countAnnoncesDone = 0;
        var countAnnonces = index.length;
        async.eachSeries(index, function (item, callback) {
            getAnnonce(item, function (err, annonce) {
                annonces.push(annonce);
                countAnnoncesDone++;
                progress(countAnnoncesDone / countAnnonces);
                callback(err, annonce);
            });
        }, function (err, result) {
            complete(err, annonces);
        })
    });

    function getIndex(complete) {

        var index = [];
        var loadNextPage = true;
        var page = 1;

        async.doWhilst(
            function iterator(callback) {
                console.log('seloger: now loading page', page);
                xray('http://www.leboncoin.fr/ventes_immobilieres/?o=' + page)
                    .select([{
                        $root: 'div.list-lbc > a',
                        lien: '[href]',
                        date: '.date div:nth-child(1)',
                        heure: '.date div:nth-child(2)'
                    }])
                    .format(function (obj) {
                        if (obj.date.trim() == 'Aujourd\'hui') {
                            obj.timestamp = moment().startOf('day');
                        } else {
                            obj.timestamp = moment(obj.date, 'DD MMM');
                        }
                        obj.timestamp = obj.timestamp.add(obj.heure.split(':')[0], 'hours').add(obj.heure.split(':')[1], 'minutes');
                        if (obj.timestamp.isBefore(options.stopAtTimestamp)) {
                            loadNextPage = false;
                        }
                        obj.timestamp = obj.timestamp.unix() * 1000;
                        obj.site_id = obj.lien.match(/(.*\/)([0-9]+)(.htm.*)/)[2];
                        delete obj.date;
                        delete obj.heure;
                        return obj;
                    })
                    .run(function (err, result) {
                        index = index.concat(result);
                        callback();
                    });
            },
            function test() {
                page++;
                return loadNextPage && page <= LeBonCoinScrapper.MAX_INDEX_PAGES;
            },
            function done() {
                complete(index);
            }
        );

    }

    function getAnnonce(item, complete) {
        xray(item.lien)
            .select({
                titre: '#ad_subject',
                pro: '.ad_pro',
                auteur: '.upload_by a',
                timestamp_init: '.upload_by',
                prix: '.price .price',
                ville: 'table+ table tr:nth-child(1) td',
                code_postal: 'table+ table tr:nth-child(2) td',
                temp_criterias_1_header: '.criterias tr:nth-child(1) th',
                temp_criterias_1_data: '.criterias tr:nth-child(1) td',
                temp_criterias_2_header: '.criterias tr:nth-child(2) th',
                temp_criterias_2_data: '.criterias tr:nth-child(2) td',
                temp_criterias_3_header: '.criterias tr:nth-child(3) th',
                temp_criterias_3_data: '.criterias tr:nth-child(3) td',
                temp_criterias_4_header: '.criterias tr:nth-child(4) th',
                temp_criterias_4_data: '.criterias tr:nth-child(4) td',
                temp_criterias_5_header: '.criterias tr:nth-child(5) th',
                temp_criterias_5_data: '.criterias tr:nth-child(5) td',
                temp_criterias_6_header: '.criterias tr:nth-child(6) th',
                temp_criterias_6_data: '.criterias tr:nth-child(6) td',
                temp_criterias_7_header: '.criterias tr:nth-child(7) th',
                temp_criterias_7_data: '.criterias tr:nth-child(7) td',
                temp_criterias_8_header: '.criterias tr:nth-child(8) th',
                temp_criterias_8_data: '.criterias tr:nth-child(8) td',
                texte: '.content',
                thumb_1: '#thumbs_carousel a:nth-child(1) .thumbs[style]',
                thumb_2: '#thumbs_carousel a:nth-child(2) .thumbs[style]',
                thumb_3: '#thumbs_carousel a:nth-child(3) .thumbs[style]',
                thumb_4: '#thumbs_carousel a:nth-child(4) .thumbs[style]',
                thumb_5: '#thumbs_carousel a:nth-child(5) .thumbs[style]',
                thumb_6: '#thumbs_carousel a:nth-child(6) .thumbs[style]',
                thumb_7: '#thumbs_carousel a:nth-child(7) .thumbs[style]',
                thumb_8: '#thumbs_carousel a:nth-child(8) .thumbs[style]',
                thumb_9: '#thumbs_carousel a:nth-child(9) .thumbs[style]',
                thumb_10: '#thumbs_carousel a:nth-child(10) .thumbs[style]',
                thumb_11: '#thumbs_carousel a:nth-child(11) .thumbs[style]',
                thumb_12: '#thumbs_carousel a:nth-child(12) .thumbs[style]',
                thumb_13: '#thumbs_carousel a:nth-child(13) .thumbs[style]',
                thumb_14: '#thumbs_carousel a:nth-child(14) .thumbs[style]',
                thumb_15: '#thumbs_carousel a:nth-child(15) .thumbs[style]'
            })
            .format(function (obj) {
                if (obj) {
                    obj.site = 'leboncoin';
                    if (obj.titre) obj.titre = obj.titre.replace(/\n/g, ' ').replace(/\s{2,}/g, ' ');
                    if (obj.pro) obj.pro = true;
                    if (obj.prix) obj.prix = obj.prix ? obj.prix.replace(/[^0-9]/g, '') : null;
                    if (obj.texte) obj.texte = obj.texte.replace(/\n/g, ' ').replace(/\s{2,}/g, ' ');
                    var extract = /Mise en ligne le (.*)\./gm.exec(obj.date_mise_en_ligne);
                    if (extract && extract[1]) {
                        obj.timestamp_init = moment(extract[1], 'DD MMM HH:mm', 'fr').unix() * 1000;
                    }
                    obj.timestamp = item.timestamp;
                    obj.lien = item.lien;
                    obj.site_id = item.lien.match(/.*ventes_immobilieres\/(.*)\.htm.*/)[1];
                    // nettoyage caractéristiques
                    var h, d;
                    for (var i = 1; i <= 8; i++) {
                        h = obj['temp_criterias_' + i + '_header'];
                        d = obj['temp_criterias_' + i + '_data'];
                        if (h) {
                            switch (h) {
                                case 'Frais d\'agence inclus : ':
                                    if (d == 'Oui') obj.frais_agence_inclus = true;
                                    break;
                                case 'Type de bien : ':
                                    obj.type = d.toLowerCase();
                                    break;
                                case 'Pièces : ':
                                    obj.pieces = parseInt(d);
                                    break;
                                case 'Surface : ':
                                    obj.surface = d.replace(/[^0-9]/g, '');
                                    break;
                                case 'Référence : ':
                                    obj.reference = d;
                                    break;
                                case 'GES : ':
                                    obj.ges = d;
                                    break;
                                case 'Classe énergie : ':
                                    obj.classe_energie = d;
                                    break;
                            }
                            delete obj['temp_criterias_' + i + '_header'];
                            delete obj['temp_criterias_' + i + '_data'];
                        }
                    }
                    // nettoyage thumbnails
                    var arr = [];
                    for (var i = 1; i <= 15; i++) {
                        d = obj['thumb_' + i];
                        if (d) {
                            arr.push(d.match(/(http:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?/)[0]);
                            delete obj['thumb_' + i];
                        }
                    }
                    if (arr.length > 0) obj.thumbs = arr;
                }
                return obj;
            })
            .run(function (err, result) {
                complete(err, result);
            });
    }
};

LeBonCoinScrapper.run(
    {},
    function progress(percent) {
        console.log('progress: ' + (percent*100).toFixed(1) + '%');
    },
    function complete(err, result) {
        //console.log('index', index, 'length', index.length);
        console.log('done, err', err, 'result', result, 'length', result.length);
    }
);
