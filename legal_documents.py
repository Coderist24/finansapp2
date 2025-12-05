# Hukuki Dökümanlar

# Kullanıcı Aydınlatma Metni
USER_TERMS_DOCUMENT = """
# 📄 Kullanıcı Aydınlatma Metni

## 1. Hizmetin Tanımı
Portföy Yönetim Sistemi ("Hizmet"), kullanıcılara finansal portföy yönetimi, analiz ve raporlama özellikleri sunmaktadır.

## 2. Kullanıcı Hakları
- Hizmetin tüm özelliklerini kullanma hakkı
- Verilerini istediği zaman görüntüleme hakkı
- Hesabını istediği zaman silme hakkı
- Destek almak için bizimle iletişim kurma hakkı

## 3. Kullanıcı Sorumlulukları
- Doğru ve güncel bilgiler sağlama
- Hesap güvenliğini koruma
- Hizmetin yasalara uygun kullanılması
- Başkalarının hesabını kullanmamak

## 4. Hizmet Şartları
- Hizmet 7/24 sunulmaya çalışılır ancak kesintisiz garanti verilmez
- Bakım ve güncellemeler için hizmet kesintileri yaşanabilir
- Hizmetin kötüye kullanımı durumunda hesap silinebilir

## 5. Sorumluluk Reddi
Hizmetin kullanımından kaynaklanan herhangi bir zarar için sorumluluk kabul edilmez.

---
**Son Güncelleme Tarihi:** 03 Aralık 2025
"""

# Gizlilik Politikası
PRIVACY_POLICY_DOCUMENT = """
# 🔒 Gizlilik Politikası

## 1. Veri Toplanması
- Ad, soyad, e-posta adresi
- Portföy bilgileri ve işlemler
- Site kullanım istatistikleri

## 2. Verilerin Kullanımı
Verileriniz aşağıdaki amaçlarla kullanılır:
- Hizmet sunumu
- Kullanıcı deneyimi iyileştirme
- Güvenlik ve dolandırıcılık tespiti
- Yasal yükümlülükleri yerine getirme

## 3. Veri Güvenliği
- SSL şifrelemesi kullanılır
- Veriler güvenli sunucularda saklanır
- Düzenli güvenlik denetimleri yapılır
- Yetkisiz erişim engellenmeye çalışılır

## 4. Üçüncü Taraflarla Paylaşım
- Verileriniz hiçbir zaman satılmaz
- Yasal zorunluluk olmadıkça paylaşılmaz
- Hizmet sağlayıcıları gizlilik anlaşmasına bağlıdır

## 5. Çerez Politikası
- Oturum çerezleri kullanılır
- Analitik çerezleri site iyileştirmek için kullanılır
- Tarayıcıdan çerezleri devre dışı bırakabilirsiniz

## 6. Veri Saklama
- Veriler hesap aktif olduğu sürece saklanır
- Hesap silme sonrası veriler 30 gün içinde silinir
- Yasal zorunluluk ise daha uzun süre saklanabilir

## 7. Haklarınız
- Verilerinizi görüntüleme
- Verilerinizi düzeltme
- Verilerinizin silinmesini isteme
- İşlemenin durdurulmasını isteme

---
**Son Güncelleme Tarihi:** 03 Aralık 2025
"""

# Elektronik İleti Politikası
COOKIE_POLICY_DOCUMENT = """
# 🍪 Elektronik İleti Politikası

## 1. Elektronik İleti Nedir?
Elektronik iletiler, e-posta, SMS ve push notification gibi dijital yollarla gönderilen haberlerdir.

## 2. İletilerin Türleri
- **Zorunlu İletiler**: Hesap güvenliği, şifre sıfırlama, önemli bildirimler
- **Tercihe Bağlı İletiler**: Pazarlama, promosyonlar, yeni özellikler
- **İstatistik İletileri**: Kullanım raporları, özet bilgiler

## 3. Sıklık
- Zorunlu iletiler: Gerektiğinde
- Pazarlama iletileri: Haftada en fazla 2 defa
- Özet raporlar: Aylık veya haftalık

## 4. Vazgeçme Hakkı
- Pazarlama iletilerinden her zaman vazgeçebilirsiniz
- İleti ayarlarınızdan tercihleri değiştirebilirsiniz
- Her iletin altındaki "Abonelikten Çık" linkine tıklayabilirsiniz
- Zorunlu iletilerden vazgeçemezsiniz

## 5. Gizlilik
- E-posta adresiniz korunmaktadır
- İstatistik için sadece anonim veriler kullanılır
- İletileriniz, istenmedikçe başkasına gösterilmez

## 6. İletişim Tercihleri
Hesap ayarlarınızdan şunları yapabilirsiniz:
- E-posta almak istediğiniz gün ve saati seçme
- İleti türlerini filtreleme
- İleti sıklığını ayarlama

---
**Son Güncelleme Tarihi:** 03 Aralık 2025
"""

def get_document(doc_type):
    """Dökümanı türüne göre döndür"""
    if doc_type == "user_terms":
        return USER_TERMS_DOCUMENT
    elif doc_type == "privacy":
        return PRIVACY_POLICY_DOCUMENT
    elif doc_type == "cookie":
        return COOKIE_POLICY_DOCUMENT
    return ""
